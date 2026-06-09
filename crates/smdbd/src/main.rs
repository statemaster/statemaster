use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use bytes::Bytes;
use clap::Parser;
use http_body_util::Full;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use smdb_engine::{Dispatcher, Engine};
use smdb_proto::constants::SERVER_VERSION;
use smdb_storage::{RedbEngine, StorageEngine};
use smdb_wire::{Server, ServerConfig, ServerMetrics};
use tokio::net::TcpListener;
use tokio::signal;
use tokio::sync::watch;
use tracing::{error, info, warn};

#[derive(Parser, Debug)]
#[command(name = "smdbd", about = "StateMaster database daemon")]
struct Args {
    /// Path to TOML config file
    #[arg(short, long, default_value = "statemaster.toml")]
    config: String,

    /// TCP address to listen on for the wire protocol
    #[arg(long, default_value = "0.0.0.0:7632")]
    listen: String,

    /// TCP address for the metrics/health HTTP server
    #[arg(long, default_value = "0.0.0.0:7633")]
    metrics_addr: String,

    /// Directory where the database file is stored
    #[arg(long, default_value = "data")]
    data_dir: String,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Path to TLS certificate (PEM). If absent, self-signed cert is generated.
    #[arg(long)]
    tls_cert: Option<String>,

    /// Path to TLS private key (PEM). If absent, self-signed cert is generated.
    #[arg(long)]
    tls_key: Option<String>,
}

// ---------------------------------------------------------------------------
// Metrics state shared between the metrics HTTP handler and the connection loop
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Health / metrics HTTP server
// ---------------------------------------------------------------------------

async fn handle_health(
    req: Request<hyper::body::Incoming>,
    metrics: Arc<ServerMetrics>,
    start: Instant,
) -> std::result::Result<Response<Full<Bytes>>, Infallible> {
    let path = req.uri().path();
    let (status, body) = match path {
        "/healthz" => (StatusCode::OK, r#"{"status":"ok"}"#.to_string()),
        "/readyz" => (StatusCode::OK, r#"{"status":"ready"}"#.to_string()),
        "/metrics" => {
            let uptime = start.elapsed().as_secs();
            let transitions =
                metrics
                    .transitions_total
                    .load(std::sync::atomic::Ordering::Relaxed);
            let conns_total =
                metrics
                    .connections_total
                    .load(std::sync::atomic::Ordering::Relaxed);
            let conns_active =
                metrics
                    .connections_active
                    .load(std::sync::atomic::Ordering::Relaxed);
            let body = format!(
                "# HELP smdbd_uptime_seconds Daemon uptime in seconds\n\
                 # TYPE smdbd_uptime_seconds gauge\n\
                 smdbd_uptime_seconds {uptime}\n\
                 # HELP smdbd_transitions_total Total transitions processed\n\
                 # TYPE smdbd_transitions_total counter\n\
                 smdbd_transitions_total {transitions}\n\
                 # HELP smdbd_connections_total Total accepted connections\n\
                 # TYPE smdbd_connections_total counter\n\
                 smdbd_connections_total {conns_total}\n\
                 # HELP smdbd_connections_active Currently active connections\n\
                 # TYPE smdbd_connections_active gauge\n\
                 smdbd_connections_active {conns_active}\n"
            );
            (StatusCode::OK, body)
        }
        _ => (StatusCode::NOT_FOUND, r#"{"error":"not found"}"#.to_string()),
    };

    let resp = Response::builder()
        .status(status)
        .header("content-type", "text/plain; charset=utf-8")
        .body(Full::new(Bytes::from(body)))
        .unwrap();
    Ok(resp)
}

async fn run_metrics_server(
    addr: SocketAddr,
    metrics: Arc<ServerMetrics>,
    mut shutdown: watch::Receiver<bool>,
) {
    let listener = match TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            error!(addr = %addr, error = %e, "failed to bind metrics server");
            return;
        }
    };
    info!(addr = %addr, "metrics/health server listening");
    let start = Instant::now();

    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    info!("metrics server shutting down");
                    return;
                }
            }
            accept = listener.accept() => {
                match accept {
                    Ok((stream, _peer)) => {
                        let metrics = Arc::clone(&metrics);
                        let start = start;
                        tokio::spawn(async move {
                            let io = TokioIo::new(stream);
                            let svc = hyper::service::service_fn(move |req| {
                                let m = Arc::clone(&metrics);
                                handle_health(req, m, start)
                            });
                            if let Err(e) = hyper::server::conn::http1::Builder::new()
                                .serve_connection(io, svc)
                                .await
                            {
                                warn!("metrics conn error: {}", e);
                            }
                        });
                    }
                    Err(e) => {
                        warn!("metrics accept error: {}", e);
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Init tracing with JSON format
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&args.log_level)),
        )
        .init();

    info!(version = SERVER_VERSION, "smdbd starting");

    // Ensure data dir exists
    std::fs::create_dir_all(&args.data_dir)
        .with_context(|| format!("creating data dir '{}'", args.data_dir))?;

    let db_path = std::path::Path::new(&args.data_dir).join("statemaster.redb");
    info!(path = %db_path.display(), "opening storage");

    let storage: Arc<dyn StorageEngine> = Arc::new(
        RedbEngine::open(&db_path)
            .with_context(|| format!("opening database at '{}'", db_path.display()))?,
    );

    let engine = Arc::new(Engine::new(Arc::clone(&storage)));

    // Graceful shutdown channel, shared by the dispatcher, metrics server, and
    // the wire server's accept loop.
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Auth tokens: opt-in. Comma-separated SMDB_AUTH_TOKENS enables token auth;
    // when empty the server runs open (dev default), matching prior behaviour.
    let auth_tokens: Vec<String> = std::env::var("SMDB_AUTH_TOKENS")
        .ok()
        .map(|s| {
            s.split(',')
                .map(|t| t.trim().to_string())
                .filter(|t| !t.is_empty())
                .collect()
        })
        .unwrap_or_default();
    if auth_tokens.is_empty() {
        info!("no SMDB_AUTH_TOKENS set; wire server accepts any token (dev mode)");
    } else {
        info!(count = auth_tokens.len(), "token auth enabled");
    }

    // Build the wire server: one connection layer providing the handshake,
    // optional TLS, auth, all four verbs, and change-stream subscriptions.
    let server_config = ServerConfig {
        listen_addr: args.listen.clone(),
        tls_cert_path: args.tls_cert.clone(),
        tls_key_path: args.tls_key.clone(),
        auth_tokens,
        max_connections: 1024,
        max_frame_size: smdb_proto::MAX_FRAME_SIZE,
    };
    let server = Server::new(server_config, Arc::clone(&engine))
        .context("building wire server")?;
    let metrics = server.metrics();

    // Dispatcher: drains the committed log into change-stream deliveries.
    {
        let eng = Arc::clone(&engine);
        let sd = shutdown_rx.clone();
        tokio::spawn(async move {
            let mut dispatcher = Dispatcher::new(eng, Duration::from_millis(100), sd);
            dispatcher.run().await;
        });
    }

    // Metrics/health server.
    {
        let metrics_addr: SocketAddr = args
            .metrics_addr
            .parse()
            .with_context(|| format!("parsing metrics addr '{}'", args.metrics_addr))?;
        let m = Arc::clone(&metrics);
        let sd = shutdown_rx.clone();
        tokio::spawn(run_metrics_server(metrics_addr, m, sd));
    }

    // Bind the wire listener and run the server until shutdown.
    let listen_addr: SocketAddr = args
        .listen
        .parse()
        .with_context(|| format!("parsing listen addr '{}'", args.listen))?;
    let listener = TcpListener::bind(listen_addr)
        .await
        .with_context(|| format!("binding to '{}'", listen_addr))?;
    info!(addr = %listen_addr, "wire server listening");

    let server_shutdown = shutdown_rx.clone();
    let server_handle =
        tokio::spawn(async move { server.serve_with_shutdown(listener, server_shutdown).await });

    // Wait for SIGINT/SIGTERM, then drive a graceful shutdown.
    let _ = signal::ctrl_c().await;
    info!("received SIGINT/SIGTERM, initiating graceful shutdown");
    engine.shutdown();
    let _ = shutdown_tx.send(true);

    match server_handle.await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => error!(error = %e, "wire server error"),
        Err(e) => error!(error = %e, "wire server task panicked"),
    }

    info!("smdbd shutdown complete");
    Ok(())
}
