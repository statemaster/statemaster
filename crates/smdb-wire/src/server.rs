use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

use futures::{SinkExt, StreamExt};
use smdb_core::prelude::{ChangeRecord, CoreError};
use smdb_engine::{Engine, EngineError, TransitionResult};
use smdb_proto::{
    codec::{decode_message, encode_message},
    frame::{Frame, FrameCodec, FrameTag},
    messages::{
        AckMessage, AuthMessage, ChangeRecordMessage, CurrentMessage, DefineMachineMessage,
        ErrorMessage, HistoryMessage, ReadyMessage, RejectionMessage, ResultMessage,
        StartupMessage, SubscribeMessage, TransitionMessage, UnsubscribeMessage,
    },
    ProtoError, PROTOCOL_VERSION, SERVER_VERSION,
};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex};
use tokio_rustls::TlsAcceptor;
use tokio_util::codec::Framed;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::config::ServerConfig;
use crate::error::{Result, WireError};
use crate::session::Session;

/// Convenience alias for the shared write-half of a framed connection.
type SharedSink<T> =
    Arc<Mutex<futures::stream::SplitSink<Framed<T, FrameCodec>, Frame>>>;

pub struct Server {
    config: ServerConfig,
    engine: Arc<Engine>,
    tls_config: Option<Arc<rustls::ServerConfig>>,
}

impl Server {
    /// Create a new server, loading TLS certificates if configured.
    pub fn new(config: ServerConfig, engine: Arc<Engine>) -> Result<Self> {
        let tls_config = build_tls_config(&config)?;
        Ok(Self {
            config,
            engine,
            tls_config,
        })
    }

    /// Bind TCP and accept client connections in a loop, spawning a task per connection.
    pub async fn run(&self) -> Result<()> {
        let listener = TcpListener::bind(&self.config.listen_addr).await?;
        info!("smdb-wire listening on {}", self.config.listen_addr);

        let semaphore = Arc::new(tokio::sync::Semaphore::new(self.config.max_connections));

        loop {
            let (stream, peer_addr) = listener.accept().await?;
            info!("accepted connection from {}", peer_addr);

            let permit = match semaphore.clone().try_acquire_owned() {
                Ok(p) => p,
                Err(_) => {
                    warn!(
                        "max connections ({}) reached, dropping {}",
                        self.config.max_connections, peer_addr
                    );
                    continue;
                }
            };

            let engine = Arc::clone(&self.engine);
            let config = self.config.clone();
            let tls_config = self.tls_config.clone();

            tokio::spawn(async move {
                let _permit = permit;
                if let Err(e) = handle_connection(stream, engine, config, tls_config).await {
                    match e {
                        WireError::ConnectionClosed => {
                            debug!("connection from {} closed", peer_addr)
                        }
                        other => error!("connection error from {}: {}", peer_addr, other),
                    }
                }
            });
        }
    }
}

/// Drive a single TCP connection through handshake, auth, and the main request loop.
async fn handle_connection(
    stream: TcpStream,
    engine: Arc<Engine>,
    config: ServerConfig,
    tls_config: Option<Arc<rustls::ServerConfig>>,
) -> Result<()> {
    if let Some(tls_cfg) = tls_config {
        let acceptor = TlsAcceptor::from(tls_cfg);
        let tls_stream = acceptor
            .accept(stream)
            .await
            .map_err(|e| WireError::Tls(e.to_string()))?;
        serve_framed(Framed::new(tls_stream, FrameCodec), engine, config).await
    } else {
        serve_framed(Framed::new(stream, FrameCodec), engine, config).await
    }
}

/// The generic per-connection handler.
async fn serve_framed<T>(
    framed: Framed<T, FrameCodec>,
    engine: Arc<Engine>,
    config: ServerConfig,
) -> Result<()>
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    // Split into read/write halves.  `tokio::sync::Mutex` is used here so
    // that the guard is `Send` and can be held across `.await` points — this
    // is required both by the subscription forwarding tasks and by the main
    // loop that sends response frames.
    let (sink, mut stream) = framed.split();
    let sink: SharedSink<T> = Arc::new(Mutex::new(sink));

    // ------------------------------------------------------------------
    // 1. Startup handshake
    // ------------------------------------------------------------------
    let startup_frame = stream
        .next()
        .await
        .ok_or(WireError::ConnectionClosed)??;

    if startup_frame.tag != FrameTag::Startup {
        send_fatal_error(&sink, "expected Startup frame").await;
        return Err(WireError::Proto(ProtoError::InvalidFrameType(
            startup_frame.tag as u8,
        )));
    }

    let startup: StartupMessage = decode_message(&startup_frame)
        .map_err(|e| WireError::Proto(ProtoError::DeserializationError(e.to_string())))?;

    if startup.protocol_version != PROTOCOL_VERSION {
        send_fatal_error(
            &sink,
            &format!(
                "protocol version mismatch: server={} client={}",
                PROTOCOL_VERSION, startup.protocol_version
            ),
        )
        .await;
        return Err(WireError::Auth(format!(
            "unsupported protocol version {}",
            startup.protocol_version
        )));
    }

    // ------------------------------------------------------------------
    // 2. Auth
    // ------------------------------------------------------------------
    let auth_frame = stream
        .next()
        .await
        .ok_or(WireError::ConnectionClosed)??;

    if auth_frame.tag != FrameTag::Auth {
        send_fatal_error(&sink, "expected Auth frame").await;
        return Err(WireError::Auth("expected Auth frame".to_string()));
    }

    let auth: AuthMessage = decode_message(&auth_frame)?;

    if !config.auth_tokens.iter().any(|t| t == &auth.token) {
        let frame = encode_message(
            FrameTag::AuthError,
            &ErrorMessage {
                message: "invalid token".to_string(),
                fatal: true,
            },
        )?;
        sink.lock().await.send(frame).await.ok();
        return Err(WireError::Auth("invalid auth token".to_string()));
    }

    // ------------------------------------------------------------------
    // 3. AuthOk + Ready
    // ------------------------------------------------------------------
    let session_id = Uuid::new_v4().to_string();
    let mut session = Session::new(session_id.clone());
    session.authenticated = true;
    session.capabilities = startup.capabilities;

    let auth_ok = encode_message(
        FrameTag::AuthOk,
        &ErrorMessage {
            message: "ok".to_string(),
            fatal: false,
        },
    )?;
    sink.lock()
        .await
        .send(auth_ok)
        .await
        .map_err(WireError::Proto)?;

    let ready = encode_message(
        FrameTag::Ready,
        &ReadyMessage {
            session_id: session_id.clone(),
            server_version: SERVER_VERSION.to_string(),
            protocol_version: PROTOCOL_VERSION,
            capabilities: vec![
                "subscribe".to_string(),
                "history".to_string(),
                "define_machine".to_string(),
            ],
        },
    )?;
    sink.lock()
        .await
        .send(ready)
        .await
        .map_err(WireError::Proto)?;

    info!("session {} authenticated", session_id);

    // ------------------------------------------------------------------
    // 4. Main request loop
    // ------------------------------------------------------------------
    loop {
        let frame = match stream.next().await {
            Some(Ok(f)) => f,
            Some(Err(e)) => {
                warn!("session {}: framing error: {}", session_id, e);
                break;
            }
            None => break,
        };

        let tag = frame.tag;
        debug!("session {}: received frame {:?}", session_id, tag);

        if tag == FrameTag::Terminate {
            debug!("session {}: client sent Terminate", session_id);
            break;
        }

        if tag == FrameTag::Subscribe {
            match handle_subscribe(&frame, &mut session, &engine, Arc::clone(&sink)).await {
                Ok(response_frames) => {
                    let mut guard = sink.lock().await;
                    for f in response_frames {
                        if let Err(e) = guard.send(f).await {
                            error!("session {}: write error: {}", session_id, e);
                            return Err(WireError::Proto(e));
                        }
                    }
                }
                Err(e) => {
                    error!("session {}: subscribe error: {}", session_id, e);
                    let err_frame = make_error_frame(&e.to_string(), false);
                    sink.lock().await.send(err_frame).await.ok();
                }
            }
            continue;
        }

        // All other frames go through handle_frame (synchronous, no await needed inside).
        let response_frames = handle_frame(&mut session, &engine, frame);
        let mut guard = sink.lock().await;
        for f in response_frames {
            if let Err(e) = guard.send(f).await {
                error!("session {}: write error: {}", session_id, e);
                return Err(WireError::Proto(e));
            }
        }
    }

    // Clean up all subscriptions for this session.
    let sub_ids: Vec<String> = session.subscriptions.keys().cloned().collect();
    for sub_id in sub_ids {
        engine.unsubscribe(&sub_id);
    }

    info!("session {} closed", session_id);
    Ok(())
}

/// Dispatch a single frame to the appropriate handler. Returns zero or more
/// response frames to be sent back to the client.
///
/// This function is deliberately synchronous — all I/O is handled by the caller
/// so that we avoid holding a `Mutex` guard across an await point here.
fn handle_frame(session: &mut Session, engine: &Arc<Engine>, frame: Frame) -> Vec<Frame> {
    match frame.tag {
        FrameTag::Ping => vec![Frame {
            tag: FrameTag::Pong,
            body: bytes::Bytes::new(),
        }],

        FrameTag::DefineMachine => {
            let msg: DefineMachineMessage = match decode_message(&frame) {
                Ok(m) => m,
                Err(e) => return vec![make_error_frame(&e.to_string(), false)],
            };
            let request_id = msg.request_id;
            match engine.define_machine(msg.definition) {
                Ok(()) => match encode_message(
                    FrameTag::Result,
                    &ResultMessage {
                        request_id,
                        payload: serde_json::json!({ "ok": true }),
                    },
                ) {
                    Ok(f) => vec![f],
                    Err(e) => vec![make_error_frame(&e.to_string(), false)],
                },
                Err(e) => vec![make_error_frame(&e.to_string(), false)],
            }
        }

        FrameTag::Transition => {
            let msg: TransitionMessage = match decode_message(&frame) {
                Ok(m) => m,
                Err(e) => return vec![make_error_frame(&e.to_string(), false)],
            };
            let request_id = msg.request_id;
            match engine.transition(
                &msg.entity_id,
                &msg.machine,
                &msg.event,
                &msg.actor,
                msg.ctx,
                msg.expected_version,
                msg.idempotency_key,
            ) {
                Ok(result) => vec![make_transition_result_frame(request_id, result)],
                Err(e) => vec![make_rejection_frame(request_id, &e)],
            }
        }

        FrameTag::Current => {
            let msg: CurrentMessage = match decode_message(&frame) {
                Ok(m) => m,
                Err(e) => return vec![make_error_frame(&e.to_string(), false)],
            };
            let request_id = msg.request_id;
            match engine.current(&msg.entity_id, &msg.machine) {
                Ok(state) => {
                    let payload = match serde_json::to_value(&state) {
                        Ok(v) => v,
                        Err(e) => return vec![make_error_frame(&e.to_string(), false)],
                    };
                    match encode_message(FrameTag::Result, &ResultMessage { request_id, payload }) {
                        Ok(f) => vec![f],
                        Err(e) => vec![make_error_frame(&e.to_string(), false)],
                    }
                }
                Err(e) => vec![make_error_frame(&e.to_string(), false)],
            }
        }

        FrameTag::History => {
            let msg: HistoryMessage = match decode_message(&frame) {
                Ok(m) => m,
                Err(e) => return vec![make_error_frame(&e.to_string(), false)],
            };
            let request_id = msg.request_id;
            match engine.history(&msg.entity_id, &msg.machine, msg.limit, msg.after_sequence) {
                Ok(records) => {
                    let payload = match serde_json::to_value(&records) {
                        Ok(v) => v,
                        Err(e) => return vec![make_error_frame(&e.to_string(), false)],
                    };
                    match encode_message(FrameTag::Result, &ResultMessage { request_id, payload }) {
                        Ok(f) => vec![f],
                        Err(e) => vec![make_error_frame(&e.to_string(), false)],
                    }
                }
                Err(e) => vec![make_error_frame(&e.to_string(), false)],
            }
        }

        FrameTag::Ack => {
            // Ack is a flow-control hint; the engine's unbounded channel does
            // not require cursor management, so we simply parse and acknowledge.
            let _msg: AckMessage = match decode_message(&frame) {
                Ok(m) => m,
                Err(e) => return vec![make_error_frame(&e.to_string(), false)],
            };
            vec![]
        }

        FrameTag::Unsubscribe => {
            let msg: UnsubscribeMessage = match decode_message(&frame) {
                Ok(m) => m,
                Err(e) => return vec![make_error_frame(&e.to_string(), false)],
            };
            engine.unsubscribe(&msg.subscription_id);
            session.subscriptions.remove(&msg.subscription_id);
            vec![]
        }

        other => {
            warn!("session {}: unexpected frame tag {:?}", session.id, other);
            vec![make_error_frame(
                &format!("unexpected frame {:?}", other),
                false,
            )]
        }
    }
}

/// Handle a `Subscribe` frame.  Separated from `handle_frame` because it needs
/// mutable access to `session.subscriptions` and needs to spawn a forwarding task.
async fn handle_subscribe<T>(
    frame: &Frame,
    session: &mut Session,
    engine: &Arc<Engine>,
    sink: SharedSink<T>,
) -> Result<Vec<Frame>>
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let msg: SubscribeMessage = decode_message(frame)?;
    let request_id = msg.request_id;
    let subscription_id = msg.subscription_id.clone();

    let rx = engine.subscribe(
        subscription_id.clone(),
        msg.machine_filter,
        msg.after_sequence,
    )?;

    // Remove any stale receiver for a re-subscription before inserting the new one.
    session.subscriptions.remove(&subscription_id);

    // Spawn a task that drains the receiver and writes ChangeRecord frames to
    // the shared sink.  The task owns both the receiver and a clone of the sink Arc.
    let sub_id_task = subscription_id.clone();
    tokio::spawn(async move {
        forward_subscription(sub_id_task, rx, sink).await;
    });

    let ok_frame = encode_message(
        FrameTag::Result,
        &ResultMessage {
            request_id,
            payload: serde_json::json!({
                "subscription_id": subscription_id,
                "ok": true,
            }),
        },
    )?;

    Ok(vec![ok_frame])
}

/// Drain `rx` and forward each `ChangeRecord` as a `ChangeRecord` frame through `sink`.
async fn forward_subscription<T>(
    subscription_id: String,
    mut rx: mpsc::UnboundedReceiver<ChangeRecord>,
    sink: SharedSink<T>,
) where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    while let Some(record) = rx.recv().await {
        let frame = match encode_message(
            FrameTag::ChangeRecord,
            &ChangeRecordMessage {
                subscription_id: subscription_id.clone(),
                record,
            },
        ) {
            Ok(f) => f,
            Err(e) => {
                error!("subscription {}: encode error: {}", subscription_id, e);
                continue;
            }
        };

        // Lock, send, release — holding the tokio Mutex across a single send is fine.
        if let Err(e) = sink.lock().await.send(frame).await {
            debug!(
                "subscription {}: write error (connection likely closed): {}",
                subscription_id, e
            );
            break;
        }
    }
    debug!("subscription {} forwarder exiting", subscription_id);
}

// ---------------------------------------------------------------------------
// Frame construction helpers
// ---------------------------------------------------------------------------

fn make_error_frame(message: &str, fatal: bool) -> Frame {
    encode_message(
        FrameTag::Error,
        &ErrorMessage {
            message: message.to_string(),
            fatal,
        },
    )
    .unwrap_or_else(|_| Frame {
        tag: FrameTag::Error,
        body: bytes::Bytes::from_static(b"encoding error"),
    })
}

fn make_transition_result_frame(request_id: u64, result: TransitionResult) -> Frame {
    let payload = serde_json::json!({
        "entity_id":     result.entity_id,
        "machine":       result.machine,
        "from":          result.from_state,
        "state":         result.to_state,
        "version":       result.version,
        "transition_id": result.transition_id,
        "sequence":      result.sequence,
        "ts":            result.timestamp.to_rfc3339(),
    });

    encode_message(FrameTag::Result, &ResultMessage { request_id, payload })
        .unwrap_or_else(|e| make_error_frame(&e.to_string(), false))
}

fn make_rejection_frame(request_id: u64, err: &EngineError) -> Frame {
    let (code, message, current_state, version): (&str, String, Option<String>, Option<u64>) =
        match err {
            EngineError::Core(CoreError::IllegalTransition { current_state, .. }) => (
                "illegal_transition",
                err.to_string(),
                Some(current_state.clone()),
                None,
            ),

            EngineError::Core(CoreError::GuardFailed { .. }) => {
                ("guard_failed", err.to_string(), None, None)
            }

            EngineError::Core(CoreError::VersionConflict { actual, .. }) => (
                "version_conflict",
                err.to_string(),
                None,
                Some(*actual),
            ),

            EngineError::Core(CoreError::UnknownMachine { .. })
            | EngineError::MachineNotFound(_) => {
                ("unknown_machine", err.to_string(), None, None)
            }

            EngineError::Core(CoreError::UnknownEntity { .. }) => {
                ("unknown_entity", err.to_string(), None, None)
            }

            _ => ("internal_error", err.to_string(), None, None),
        };

    encode_message(
        FrameTag::Rejection,
        &RejectionMessage {
            request_id,
            code: code.to_string(),
            message,
            current_state,
            version,
        },
    )
    .unwrap_or_else(|e| make_error_frame(&e.to_string(), false))
}

/// Send a fatal error frame — best-effort, ignores write errors.
async fn send_fatal_error<T>(sink: &SharedSink<T>, message: &str)
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let frame = make_error_frame(message, true);
    sink.lock().await.send(frame).await.ok();
}

// ---------------------------------------------------------------------------
// TLS config builder
// ---------------------------------------------------------------------------

fn build_tls_config(config: &ServerConfig) -> Result<Option<Arc<rustls::ServerConfig>>> {
    let (cert_path, key_path) = match (&config.tls_cert_path, &config.tls_key_path) {
        (Some(c), Some(k)) => (c, k),
        _ => return Ok(None),
    };

    let certs = {
        let f = File::open(cert_path)
            .map_err(|e| WireError::Tls(format!("failed to open cert file: {}", e)))?;
        rustls_pemfile::certs(&mut BufReader::new(f))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| WireError::Tls(format!("failed to parse certs: {}", e)))?
    };

    let key = {
        let f = File::open(key_path)
            .map_err(|e| WireError::Tls(format!("failed to open key file: {}", e)))?;
        let mut reader = BufReader::new(f);
        let mut keys: Vec<rustls::pki_types::PrivateKeyDer<'static>> = Vec::new();

        for item in rustls_pemfile::read_all(&mut reader) {
            match item
                .map_err(|e| WireError::Tls(format!("failed to parse key file: {}", e)))?
            {
                rustls_pemfile::Item::Pkcs1Key(k) => {
                    keys.push(rustls::pki_types::PrivateKeyDer::Pkcs1(k))
                }
                rustls_pemfile::Item::Pkcs8Key(k) => {
                    keys.push(rustls::pki_types::PrivateKeyDer::Pkcs8(k))
                }
                rustls_pemfile::Item::Sec1Key(k) => {
                    keys.push(rustls::pki_types::PrivateKeyDer::Sec1(k))
                }
                _ => {}
            }
        }

        keys.into_iter()
            .next()
            .ok_or_else(|| WireError::Tls("no private key found in key file".to_string()))?
    };

    let tls_cfg = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| WireError::Tls(format!("failed to build TLS config: {}", e)))?;

    Ok(Some(Arc::new(tls_cfg)))
}
