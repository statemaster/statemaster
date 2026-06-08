use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use bytes::Bytes;
use clap::Parser;
use futures::SinkExt;
use futures::StreamExt;
use http_body_util::Full;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use smdb_engine::{Dispatcher, Engine};
use smdb_proto::constants::{PROTOCOL_VERSION, SERVER_VERSION};
use smdb_proto::frame::{Frame, FrameCodec, FrameTag};
use smdb_proto::messages::{
    AuthMessage, CurrentMessage, DefineMachineMessage, ErrorMessage, HistoryMessage, ReadyMessage,
    RejectionMessage, ResultMessage, StartupMessage, TransitionMessage,
};
use smdb_proto::{decode_message, encode_message};
use smdb_storage::{RedbEngine, StorageEngine};
use tokio::net::{TcpListener, TcpStream};
use tokio::signal;
use tokio::sync::watch;
use tokio_util::codec::Framed;
use tracing::{error, info, warn};
use uuid::Uuid;

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

#[derive(Default)]
struct Metrics {
    transitions_total: std::sync::atomic::AtomicU64,
    connections_total: std::sync::atomic::AtomicU64,
    connections_active: std::sync::atomic::AtomicI64,
}

// ---------------------------------------------------------------------------
// Health / metrics HTTP server
// ---------------------------------------------------------------------------

async fn handle_health(
    req: Request<hyper::body::Incoming>,
    metrics: Arc<Metrics>,
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
    metrics: Arc<Metrics>,
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
// Wire protocol session handler
// ---------------------------------------------------------------------------

/// Handle a single client connection: perform the handshake then route frames
/// to the engine until the client disconnects or the server shuts down.
async fn handle_connection(
    stream: TcpStream,
    engine: Arc<Engine>,
    metrics: Arc<Metrics>,
    server_token: Option<String>,
    mut shutdown: watch::Receiver<bool>,
) {
    metrics
        .connections_total
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    metrics
        .connections_active
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    let peer = stream.peer_addr().ok();
    let session_id = Uuid::new_v4().to_string();
    info!(session_id = %session_id, peer = ?peer, "client connected");

    let mut framed: Framed<TcpStream, FrameCodec> = Framed::new(stream, FrameCodec);

    // ------------------------------------------------------------------
    // Handshake: Startup → Auth → AuthOk → Ready
    // ------------------------------------------------------------------

    // 1. Expect Startup
    let startup_frame = match next_frame(&mut framed, &mut shutdown).await {
        Some(Ok(f)) => f,
        _ => {
            warn!(session_id = %session_id, "client disconnected before startup");
            decrement_active(&metrics);
            return;
        }
    };

    if startup_frame.tag != FrameTag::Startup {
        send_error(&mut framed, "expected Startup frame", true).await;
        decrement_active(&metrics);
        return;
    }

    let startup: StartupMessage = match decode_message(&startup_frame) {
        Ok(m) => m,
        Err(e) => {
            send_error(&mut framed, &format!("malformed Startup: {e}"), true).await;
            decrement_active(&metrics);
            return;
        }
    };

    if startup.protocol_version != PROTOCOL_VERSION {
        send_error(
            &mut framed,
            &format!(
                "unsupported protocol version {} (server speaks {})",
                startup.protocol_version, PROTOCOL_VERSION
            ),
            true,
        )
        .await;
        decrement_active(&metrics);
        return;
    }

    // 2. Expect Auth
    let auth_frame = match next_frame(&mut framed, &mut shutdown).await {
        Some(Ok(f)) => f,
        _ => {
            warn!(session_id = %session_id, "client disconnected before auth");
            decrement_active(&metrics);
            return;
        }
    };

    if auth_frame.tag != FrameTag::Auth {
        send_error(&mut framed, "expected Auth frame", true).await;
        decrement_active(&metrics);
        return;
    }

    let auth: AuthMessage = match decode_message(&auth_frame) {
        Ok(m) => m,
        Err(e) => {
            send_error(&mut framed, &format!("malformed Auth: {e}"), true).await;
            decrement_active(&metrics);
            return;
        }
    };

    // Validate token if one is configured
    if let Some(ref expected) = server_token {
        if &auth.token != expected {
            let err_frame = encode_message(
                FrameTag::AuthError,
                &ErrorMessage {
                    message: "invalid token".to_string(),
                    fatal: true,
                },
            )
            .unwrap();
            let _ = framed.send(err_frame).await;
            warn!(session_id = %session_id, "auth failed: bad token");
            decrement_active(&metrics);
            return;
        }
    }

    // 3. Send AuthOk
    let auth_ok = Frame {
        tag: FrameTag::AuthOk,
        body: Bytes::new(),
    };
    if framed.send(auth_ok).await.is_err() {
        decrement_active(&metrics);
        return;
    }

    // 4. Send Ready
    let ready = encode_message(
        FrameTag::Ready,
        &ReadyMessage {
            session_id: session_id.clone(),
            server_version: SERVER_VERSION.to_string(),
            protocol_version: PROTOCOL_VERSION,
            capabilities: vec!["subscribe".into(), "history".into()],
        },
    )
    .unwrap();
    if framed.send(ready).await.is_err() {
        decrement_active(&metrics);
        return;
    }

    info!(session_id = %session_id, "handshake complete");

    // ------------------------------------------------------------------
    // Main request loop
    // ------------------------------------------------------------------
    loop {
        let frame = match next_frame(&mut framed, &mut shutdown).await {
            Some(Ok(f)) => f,
            Some(Err(e)) => {
                warn!(session_id = %session_id, error = %e, "frame decode error");
                break;
            }
            None => break, // shutdown or EOF
        };

        match frame.tag {
            FrameTag::Transition => {
                let msg: TransitionMessage = match decode_message(&frame) {
                    Ok(m) => m,
                    Err(e) => {
                        send_error(&mut framed, &format!("malformed Transition: {e}"), false)
                            .await;
                        continue;
                    }
                };
                let rid = msg.request_id;
                match engine.transition(
                    &msg.entity_id,
                    &msg.machine,
                    &msg.event,
                    &msg.actor,
                    msg.ctx,
                    msg.expected_version,
                    msg.idempotency_key,
                ) {
                    Ok(result) => {
                        metrics
                            .transitions_total
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let payload = serde_json::to_value(&result).unwrap_or_default();
                        let resp = encode_message(
                            FrameTag::Result,
                            &ResultMessage {
                                request_id: rid,
                                payload,
                            },
                        )
                        .unwrap();
                        if framed.send(resp).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        let (code, current_state, version) = classify_engine_error(&e);
                        let msg_str = e.to_string();
                        let rej = encode_message(
                            FrameTag::Rejection,
                            &RejectionMessage {
                                request_id: rid,
                                code,
                                message: msg_str,
                                current_state,
                                version,
                            },
                        )
                        .unwrap();
                        if framed.send(rej).await.is_err() {
                            break;
                        }
                    }
                }
            }

            FrameTag::Current => {
                let msg: CurrentMessage = match decode_message(&frame) {
                    Ok(m) => m,
                    Err(e) => {
                        send_error(&mut framed, &format!("malformed Current: {e}"), false).await;
                        continue;
                    }
                };
                let rid = msg.request_id;
                match engine.current(&msg.entity_id, &msg.machine) {
                    Ok(state) => {
                        let payload = serde_json::to_value(&state).unwrap_or_default();
                        let resp = encode_message(
                            FrameTag::Result,
                            &ResultMessage {
                                request_id: rid,
                                payload,
                            },
                        )
                        .unwrap();
                        if framed.send(resp).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        let err_resp = encode_message(
                            FrameTag::Error,
                            &ErrorMessage {
                                message: e.to_string(),
                                fatal: false,
                            },
                        )
                        .unwrap();
                        if framed.send(err_resp).await.is_err() {
                            break;
                        }
                    }
                }
            }

            FrameTag::History => {
                let msg: HistoryMessage = match decode_message(&frame) {
                    Ok(m) => m,
                    Err(e) => {
                        send_error(&mut framed, &format!("malformed History: {e}"), false).await;
                        continue;
                    }
                };
                let rid = msg.request_id;
                match engine.history(
                    &msg.entity_id,
                    &msg.machine,
                    msg.limit,
                    msg.after_sequence,
                ) {
                    Ok(records) => {
                        let payload = serde_json::to_value(&records).unwrap_or_default();
                        let resp = encode_message(
                            FrameTag::Result,
                            &ResultMessage {
                                request_id: rid,
                                payload,
                            },
                        )
                        .unwrap();
                        if framed.send(resp).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        let err_resp = encode_message(
                            FrameTag::Error,
                            &ErrorMessage {
                                message: e.to_string(),
                                fatal: false,
                            },
                        )
                        .unwrap();
                        if framed.send(err_resp).await.is_err() {
                            break;
                        }
                    }
                }
            }

            FrameTag::DefineMachine => {
                let msg: DefineMachineMessage = match decode_message(&frame) {
                    Ok(m) => m,
                    Err(e) => {
                        send_error(&mut framed, &format!("malformed DefineMachine: {e}"), false)
                            .await;
                        continue;
                    }
                };
                let rid = msg.request_id;
                match engine.define_machine(msg.definition) {
                    Ok(()) => {
                        let resp = encode_message(
                            FrameTag::Result,
                            &ResultMessage {
                                request_id: rid,
                                payload: serde_json::json!({"ok": true}),
                            },
                        )
                        .unwrap();
                        if framed.send(resp).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        let rej = encode_message(
                            FrameTag::Rejection,
                            &RejectionMessage {
                                request_id: rid,
                                code: "invalid_definition".to_string(),
                                message: e.to_string(),
                                current_state: None,
                                version: None,
                            },
                        )
                        .unwrap();
                        if framed.send(rej).await.is_err() {
                            break;
                        }
                    }
                }
            }

            FrameTag::Ping => {
                let pong = Frame {
                    tag: FrameTag::Pong,
                    body: Bytes::new(),
                };
                if framed.send(pong).await.is_err() {
                    break;
                }
            }

            FrameTag::Terminate => {
                info!(session_id = %session_id, "client requested terminate");
                break;
            }

            FrameTag::Subscribe | FrameTag::Ack | FrameTag::Unsubscribe => {
                // Subscriptions are not yet wired in this handler; send a notice.
                let notice = encode_message(
                    FrameTag::Notice,
                    &smdb_proto::messages::NoticeMessage {
                        message: "subscriptions not yet supported in this build".to_string(),
                    },
                )
                .unwrap();
                if framed.send(notice).await.is_err() {
                    break;
                }
            }

            other => {
                warn!(session_id = %session_id, tag = ?other, "unexpected frame from client");
            }
        }
    }

    info!(session_id = %session_id, "client disconnected");
    decrement_active(&metrics);
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn next_frame(
    framed: &mut Framed<TcpStream, FrameCodec>,
    shutdown: &mut watch::Receiver<bool>,
) -> Option<std::result::Result<Frame, smdb_proto::ProtoError>> {
    tokio::select! {
        _ = shutdown.changed() => {
            if *shutdown.borrow() {
                return None;
            }
            framed.next().await
        }
        frame = framed.next() => frame,
    }
}

async fn send_error(framed: &mut Framed<TcpStream, FrameCodec>, msg: &str, fatal: bool) {
    if let Ok(frame) = encode_message(
        FrameTag::Error,
        &ErrorMessage {
            message: msg.to_string(),
            fatal,
        },
    ) {
        let _ = framed.send(frame).await;
    }
}

fn decrement_active(metrics: &Arc<Metrics>) {
    metrics
        .connections_active
        .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
}

/// Map engine errors to rejection codes for the wire protocol.
fn classify_engine_error(
    e: &smdb_engine::EngineError,
) -> (String, Option<String>, Option<u64>) {
    use smdb_core::error::CoreError;
    use smdb_engine::EngineError;

    match e {
        EngineError::Core(CoreError::IllegalTransition { current_state, .. }) => (
            "illegal_transition".to_string(),
            Some(current_state.clone()),
            None,
        ),
        EngineError::Core(CoreError::GuardFailed { .. }) => {
            ("guard_failed".to_string(), None, None)
        }
        EngineError::Core(CoreError::VersionConflict { actual, .. }) => (
            "version_conflict".to_string(),
            None,
            Some(*actual),
        ),
        EngineError::Storage(smdb_storage::StorageError::NotFound(_)) => {
            ("unknown_entity".to_string(), None, None)
        }
        EngineError::MachineNotFound(_) => ("unknown_machine".to_string(), None, None),
        _ => ("internal_error".to_string(), None, None),
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

    // Log TLS config note (TLS termination can be layered by a proxy; rcgen
    // certs are generated here purely for dev awareness — actual TLS wrapping
    // of the wire server requires tokio-rustls and is left for the wire crate).
    if args.tls_cert.is_none() && args.tls_key.is_none() {
        info!("no TLS cert/key provided; generating self-signed cert for reference");
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
            .context("generating self-signed cert")?;
        let _pem = cert.cert.pem();
        info!("self-signed cert generated (wire server runs plain TCP; use a TLS proxy for production)");
    }

    // Graceful shutdown channel
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Dispatcher
    {
        let storage_clone = Arc::clone(&storage);
        let shutdown = engine.shutdown_rx();
        let interval = Duration::from_millis(100);
        tokio::spawn(async move {
            let mut dispatcher = Dispatcher::new(storage_clone, interval, shutdown);
            dispatcher.run().await;
        });
    }

    // Metrics/health server
    let metrics = Arc::new(Metrics::default());
    {
        let metrics_addr: SocketAddr = args
            .metrics_addr
            .parse()
            .with_context(|| format!("parsing metrics addr '{}'", args.metrics_addr))?;
        let m = Arc::clone(&metrics);
        let sd = shutdown_rx.clone();
        tokio::spawn(run_metrics_server(metrics_addr, m, sd));
    }

    // Wire protocol listener
    let listen_addr: SocketAddr = args
        .listen
        .parse()
        .with_context(|| format!("parsing listen addr '{}'", args.listen))?;
    let listener = TcpListener::bind(listen_addr)
        .await
        .with_context(|| format!("binding to '{}'", listen_addr))?;
    info!(addr = %listen_addr, "wire server listening");

    // Accept loop
    let engine_ref = Arc::clone(&engine);
    let metrics_ref = Arc::clone(&metrics);
    let server_token: Option<String> = None; // Token auth is opt-in; wired via config in a future version

    let mut shutdown_accept = shutdown_rx.clone();

    loop {
        tokio::select! {
            _ = shutdown_accept.changed() => {
                if *shutdown_accept.borrow() {
                    info!("accept loop shutting down");
                    break;
                }
            }

            // SIGTERM / SIGINT
            _ = signal::ctrl_c() => {
                info!("received SIGINT/SIGTERM, initiating graceful shutdown");
                engine.shutdown();
                let _ = shutdown_tx.send(true);
                break;
            }

            accept = listener.accept() => {
                match accept {
                    Ok((stream, peer)) => {
                        info!(peer = %peer, "accepted connection");
                        stream.set_nodelay(true).ok();
                        let eng = Arc::clone(&engine_ref);
                        let met = Arc::clone(&metrics_ref);
                        let tok = server_token.clone();
                        let sd = shutdown_rx.clone();
                        tokio::spawn(handle_connection(stream, eng, met, tok, sd));
                    }
                    Err(e) => {
                        error!(error = %e, "accept error");
                    }
                }
            }
        }
    }

    info!("smdbd shutdown complete");
    Ok(())
}
