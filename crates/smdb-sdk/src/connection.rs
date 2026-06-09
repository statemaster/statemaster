use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio_util::codec::Framed;

use smdb_proto::constants::PROTOCOL_VERSION;
use smdb_proto::frame::{Frame, FrameTag};
use smdb_proto::messages::{AuthMessage, StartupMessage};
use smdb_proto::{decode_message, encode_message, FrameCodec};

use crate::config::ClientConfig;
use crate::error::{Result, SdkError};

// Sender side is stored per subscription_id.
pub(crate) type SubscriptionTx = mpsc::UnboundedSender<smdb_core::prelude::ChangeRecord>;

/// Internal state shared between the write-half of a connection and its
/// background reader task.
pub(crate) struct SharedState {
    /// Pending request/response pairs keyed by request_id.
    pub pending: HashMap<u64, oneshot::Sender<Frame>>,
    /// Active subscriptions keyed by subscription_id.
    pub subscriptions: HashMap<String, SubscriptionTx>,
    /// Set to true once the reader task exits so callers know the connection is gone.
    pub dead: bool,
}

impl SharedState {
    fn new() -> Self {
        Self {
            pending: HashMap::new(),
            subscriptions: HashMap::new(),
            dead: false,
        }
    }
}

/// A single TCP (or TLS) connection to a StateMaster server.
pub(crate) struct Connection {
    /// Framed write half; wrapped in a Mutex so callers can send concurrently.
    writer: Mutex<futures::stream::SplitSink<Framed<TcpStream, FrameCodec>, Frame>>,
    next_request_id: AtomicU64,
    pub(crate) state: Arc<Mutex<SharedState>>,
}

impl Connection {
    /// Connect, handshake (Startup + Auth), and wait for AuthOk + Ready.
    pub async fn connect(config: &ClientConfig) -> Result<Arc<Self>> {
        // ---- TCP connect with timeout ----
        let tcp = tokio::time::timeout(config.connect_timeout, TcpStream::connect(&config.addr))
            .await
            .map_err(|_| {
                SdkError::ConnectionFailed(format!(
                    "connect timeout after {:?}",
                    config.connect_timeout
                ))
            })?
            .map_err(|e| SdkError::ConnectionFailed(e.to_string()))?;

        tcp.set_nodelay(true).ok();

        // TODO: TLS branch — for now only plain TCP is wired up.
        // When config.tls is true this should wrap `tcp` in a TlsConnector.
        if config.tls {
            return Err(SdkError::Internal(
                "TLS support requires tokio-rustls integration (not yet wired)".into(),
            ));
        }

        let framed = Framed::new(tcp, FrameCodec);
        let (sink, mut stream) = framed.split();

        let shared = Arc::new(Mutex::new(SharedState::new()));

        // ---- Startup frame ----
        {
            let startup = StartupMessage {
                protocol_version: PROTOCOL_VERSION,
                capabilities: vec!["subscribe".into(), "history".into()],
            };
            let frame = encode_message(FrameTag::Startup, &startup)?;
            // Temporarily take the sink to send the startup; we reassemble after.
            // We use a local variable so we don't need the Mutex yet.
            let mut s = sink;
            s.send(frame)
                .await
                .map_err(|e| SdkError::ConnectionFailed(e.to_string()))?;

            // ---- Auth frame ----
            let auth = AuthMessage {
                token: config.token.clone(),
            };
            let auth_frame = encode_message(FrameTag::Auth, &auth)?;
            s.send(auth_frame)
                .await
                .map_err(|e| SdkError::ConnectionFailed(e.to_string()))?;

            // ---- Wait for AuthOk then Ready ----
            let mut got_auth_ok = false;
            let mut got_ready = false;
            let deadline = tokio::time::Instant::now() + config.connect_timeout;

            while !(got_auth_ok && got_ready) {
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    return Err(SdkError::ConnectionFailed("handshake timeout".into()));
                }
                let frame = tokio::time::timeout(remaining, stream.next())
                    .await
                    .map_err(|_| SdkError::ConnectionFailed("handshake timeout".into()))?
                    .ok_or_else(|| {
                        SdkError::ConnectionFailed(
                            "server closed connection during handshake".into(),
                        )
                    })?
                    .map_err(|e| SdkError::ConnectionFailed(e.to_string()))?;

                match frame.tag {
                    FrameTag::AuthOk => {
                        got_auth_ok = true;
                    }
                    FrameTag::AuthError => {
                        let msg: smdb_proto::messages::ErrorMessage = decode_message(&frame)
                            .unwrap_or(smdb_proto::messages::ErrorMessage {
                                message: "auth error".into(),
                                fatal: true,
                            });
                        return Err(SdkError::AuthFailed(msg.message));
                    }
                    FrameTag::Ready => {
                        got_ready = true;
                    }
                    FrameTag::Error => {
                        let msg: smdb_proto::messages::ErrorMessage = decode_message(&frame)
                            .unwrap_or(smdb_proto::messages::ErrorMessage {
                                message: "server error".into(),
                                fatal: true,
                            });
                        return Err(SdkError::ConnectionFailed(msg.message));
                    }
                    other => {
                        tracing::warn!("unexpected frame during handshake: {:?}", other);
                    }
                }
            }

            let conn = Arc::new(Self {
                writer: Mutex::new(s),
                next_request_id: AtomicU64::new(1),
                state: shared.clone(),
            });

            // Spawn the reader task.
            Self::spawn_reader(stream, shared);

            Ok(conn)
        }
    }

    /// Send a framed request and wait for the matching response frame.
    pub async fn send_request(
        &self,
        tag: FrameTag,
        body: bytes::Bytes,
        request_id: u64,
        timeout: std::time::Duration,
    ) -> Result<Frame> {
        let (tx, rx) = oneshot::channel();

        {
            let mut state = self.state.lock().await;
            if state.dead {
                return Err(SdkError::Disconnected);
            }
            state.pending.insert(request_id, tx);
        }

        let frame = Frame { tag, body };
        {
            let mut writer = self.writer.lock().await;
            writer
                .send(frame)
                .await
                .map_err(|_| SdkError::Disconnected)?;
        }

        let result = tokio::time::timeout(timeout, rx).await;

        match result {
            Ok(Ok(frame)) => Ok(frame),
            Ok(Err(_)) => {
                // Sender dropped — connection died.
                Err(SdkError::Disconnected)
            }
            Err(_elapsed) => {
                // Remove the pending entry so the reader doesn't try to use it.
                let mut state = self.state.lock().await;
                state.pending.remove(&request_id);
                Err(SdkError::Timeout)
            }
        }
    }

    /// Allocate a fresh monotonically-increasing request ID.
    pub fn next_request_id(&self) -> u64 {
        self.next_request_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Send a raw frame without expecting a response (used for Ack/Unsubscribe).
    #[allow(dead_code)]
    pub async fn send_fire_and_forget(&self, tag: FrameTag, body: bytes::Bytes) -> Result<()> {
        let frame = Frame { tag, body };
        let mut writer = self.writer.lock().await;
        writer.send(frame).await.map_err(|_| SdkError::Disconnected)
    }

    /// Spawn a background task that reads frames from the server and routes
    /// them to the appropriate oneshot or mpsc channel.
    fn spawn_reader(
        mut stream: futures::stream::SplitStream<Framed<TcpStream, FrameCodec>>,
        shared: Arc<Mutex<SharedState>>,
    ) {
        tokio::spawn(async move {
            loop {
                let frame = match stream.next().await {
                    Some(Ok(f)) => f,
                    Some(Err(e)) => {
                        tracing::error!("connection read error: {}", e);
                        break;
                    }
                    None => {
                        tracing::info!("server closed connection");
                        break;
                    }
                };

                match frame.tag {
                    FrameTag::Result | FrameTag::Rejection | FrameTag::Error => {
                        // Extract request_id from the first 8 bytes of body (u64 BE).
                        // All three message types begin with request_id in their
                        // msgpack encoding, so we decode generically.
                        let rid = extract_request_id(&frame);
                        if let Some(rid) = rid {
                            let mut state = shared.lock().await;
                            if let Some(tx) = state.pending.remove(&rid) {
                                let _ = tx.send(frame);
                            }
                        }
                    }
                    FrameTag::ChangeRecord => {
                        // Decode to get the subscription_id.
                        match smdb_proto::decode_message::<smdb_proto::messages::ChangeRecordMessage>(
                            &frame,
                        ) {
                            Ok(msg) => {
                                let state = shared.lock().await;
                                if let Some(tx) = state.subscriptions.get(&msg.subscription_id) {
                                    let _ = tx.send(msg.record);
                                }
                            }
                            Err(e) => {
                                tracing::warn!("failed to decode ChangeRecord: {}", e);
                            }
                        }
                    }
                    FrameTag::Pong => {
                        // Keepalive response — nothing to do.
                    }
                    FrameTag::Notice => {
                        if let Ok(msg) = smdb_proto::decode_message::<
                            smdb_proto::messages::NoticeMessage,
                        >(&frame)
                        {
                            tracing::info!("server notice: {}", msg.message);
                        }
                    }
                    other => {
                        tracing::debug!("unexpected frame from server: {:?}", other);
                    }
                }
            }

            // Mark the connection as dead and wake all pending waiters.
            let mut state = shared.lock().await;
            state.dead = true;
            // Drop all pending senders so their receivers unblock with an error.
            state.pending.clear();
        });
    }
}

/// Decode just the `request_id` field from a Result/Rejection/Error frame body
/// using msgpack. All three message types serialize `request_id` as the first
/// named field, so we decode into a minimal helper struct.
fn extract_request_id(frame: &Frame) -> Option<u64> {
    #[derive(serde::Deserialize)]
    struct WithRequestId {
        request_id: u64,
    }
    rmp_serde::from_slice::<WithRequestId>(&frame.body)
        .ok()
        .map(|m| m.request_id)
}
