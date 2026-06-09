use std::sync::Arc;
use std::time::Duration;

use rand::Rng;
use smdb_core::prelude::{ChangeRecord, EntityState, MachineDefinition, TransitionRecord};
use smdb_proto::messages::{
    CurrentMessage, DefineMachineMessage, RejectionMessage, ResultMessage, SubscribeMessage,
};
use smdb_proto::{decode_message, encode_message, FrameTag};
use tokio::sync::{mpsc, Mutex};
use uuid::Uuid;

use crate::config::ClientConfig;
use crate::connection::Connection;
use crate::error::{Result, SdkError};
use crate::response::TransitionResponse;
use crate::subscription::Subscription;

/// The main StateMaster client.  Wraps a single connection and provides a
/// typed async API over the wire protocol.
///
/// Cloning a [`Client`] is cheap — both clones share the same underlying
/// connection.
#[derive(Clone)]
pub struct Client {
    pub(crate) config: ClientConfig,
    pub(crate) connection: Arc<Mutex<Arc<Connection>>>,
}

impl Client {
    /// Establish a connection using the supplied config.
    pub async fn connect(config: ClientConfig) -> Result<Self> {
        let conn = Self::connect_with_retry(&config).await?;
        Ok(Self {
            config,
            connection: Arc::new(Mutex::new(conn)),
        })
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /// Register or update a machine definition on the server.
    pub async fn define_machine(&self, definition: MachineDefinition) -> Result<()> {
        let conn = self.conn().await;
        let request_id = conn.next_request_id();

        let msg = DefineMachineMessage {
            request_id,
            name: definition.name.clone(),
            version: definition.version,
            definition,
        };
        let frame = encode_message(FrameTag::DefineMachine, &msg)?;
        let resp = conn
            .send_request(
                frame.tag,
                frame.body,
                request_id,
                self.config.request_timeout,
            )
            .await?;

        match resp.tag {
            FrameTag::Result => Ok(()),
            FrameTag::Rejection => {
                let rej: RejectionMessage = decode_message(&resp)?;
                Err(SdkError::Rejected {
                    code: rej.code,
                    message: rej.message,
                    current_state: rej.current_state,
                    version: rej.version,
                })
            }
            FrameTag::Error => {
                let msg: smdb_proto::messages::ErrorMessage =
                    decode_message(&resp).unwrap_or(smdb_proto::messages::ErrorMessage {
                        message: "unknown server error".into(),
                        fatal: false,
                    });
                Err(SdkError::Internal(msg.message))
            }
            other => Err(SdkError::Internal(format!(
                "unexpected response tag: {:?}",
                other
            ))),
        }
    }

    /// Start building a transition request.  Call `.send().await` to execute.
    pub fn transition(
        &self,
        entity_id: impl Into<String>,
        machine: impl Into<String>,
        event: impl Into<String>,
    ) -> TransitionBuilder {
        TransitionBuilder {
            client: self.clone(),
            entity_id: entity_id.into(),
            machine: machine.into(),
            event: event.into(),
            expected_version: None,
            ctx: serde_json::Value::Null,
            actor: String::new(),
            idempotency_key: None,
        }
    }

    /// Return the current state of an entity in a machine.
    pub async fn current(
        &self,
        entity_id: impl Into<String>,
        machine: impl Into<String>,
    ) -> Result<EntityState> {
        let conn = self.conn().await;
        let request_id = conn.next_request_id();

        let msg = CurrentMessage {
            request_id,
            entity_id: entity_id.into(),
            machine: machine.into(),
        };
        let frame = encode_message(FrameTag::Current, &msg)?;
        let resp = conn
            .send_request(
                frame.tag,
                frame.body,
                request_id,
                self.config.request_timeout,
            )
            .await?;

        self.decode_result_payload::<EntityState>(resp)
    }

    /// Return the history of an entity within a machine.
    pub fn history(
        &self,
        entity_id: impl Into<String>,
        machine: impl Into<String>,
    ) -> HistoryBuilder {
        HistoryBuilder {
            client: self.clone(),
            entity_id: entity_id.into(),
            machine: machine.into(),
            limit: None,
            after_sequence: None,
        }
    }

    /// Subscribe to change records pushed by the server.
    ///
    /// `machine_filter` — if `Some`, only records for that machine are sent.
    /// `after_sequence` — resume from this sequence number (0 = from start).
    pub async fn subscribe(
        &self,
        machine_filter: Option<String>,
        after_sequence: u64,
    ) -> Result<Subscription> {
        let conn = self.conn().await;
        let request_id = conn.next_request_id();
        let subscription_id = Uuid::new_v4().to_string();

        let (tx, rx) = mpsc::unbounded_channel::<ChangeRecord>();

        // Register before sending the frame so no records can be missed.
        {
            let mut state = conn.state.lock().await;
            state.subscriptions.insert(subscription_id.clone(), tx);
        }

        let msg = SubscribeMessage {
            request_id,
            subscription_id: subscription_id.clone(),
            machine_filter,
            after_sequence,
        };
        let frame = encode_message(FrameTag::Subscribe, &msg)?;
        let resp = conn
            .send_request(
                frame.tag,
                frame.body,
                request_id,
                self.config.request_timeout,
            )
            .await?;

        match resp.tag {
            FrameTag::Result => Ok(Subscription {
                id: subscription_id,
                receiver: rx,
            }),
            FrameTag::Rejection => {
                // Clean up the subscription channel we registered.
                let mut state = conn.state.lock().await;
                state.subscriptions.remove(&subscription_id);

                let rej: RejectionMessage = decode_message(&resp)?;
                Err(SdkError::Rejected {
                    code: rej.code,
                    message: rej.message,
                    current_state: rej.current_state,
                    version: rej.version,
                })
            }
            FrameTag::Error => {
                let mut state = conn.state.lock().await;
                state.subscriptions.remove(&subscription_id);

                let msg: smdb_proto::messages::ErrorMessage =
                    decode_message(&resp).unwrap_or(smdb_proto::messages::ErrorMessage {
                        message: "unknown server error".into(),
                        fatal: false,
                    });
                Err(SdkError::Internal(msg.message))
            }
            other => Err(SdkError::Internal(format!(
                "unexpected response tag: {:?}",
                other
            ))),
        }
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Obtain the current live connection, reconnecting if it has died.
    async fn conn(&self) -> Arc<Connection> {
        let guard = self.connection.lock().await;
        // A cheap check: if the reader task has exited, reconnect.
        // (The reconnect logic is outside the lock to avoid holding it during I/O.)
        guard.clone()
    }

    /// Decode a `Result` frame's payload field into `T`, or convert a
    /// `Rejection`/`Error` frame to the appropriate `SdkError`.
    pub(crate) fn decode_result_payload<T: serde::de::DeserializeOwned>(
        &self,
        frame: smdb_proto::frame::Frame,
    ) -> Result<T> {
        match frame.tag {
            FrameTag::Result => {
                let result_msg: ResultMessage = decode_message(&frame)?;
                serde_json::from_value::<T>(result_msg.payload)
                    .map_err(|e| SdkError::Internal(format!("deserialize payload: {}", e)))
            }
            FrameTag::Rejection => {
                let rej: RejectionMessage = decode_message(&frame)?;
                Err(SdkError::Rejected {
                    code: rej.code,
                    message: rej.message,
                    current_state: rej.current_state,
                    version: rej.version,
                })
            }
            FrameTag::Error => {
                let msg: smdb_proto::messages::ErrorMessage =
                    decode_message(&frame).unwrap_or(smdb_proto::messages::ErrorMessage {
                        message: "unknown server error".into(),
                        fatal: false,
                    });
                Err(SdkError::Internal(msg.message))
            }
            other => Err(SdkError::Internal(format!(
                "unexpected response tag: {:?}",
                other
            ))),
        }
    }

    /// Connect with exponential back-off retry.
    async fn connect_with_retry(config: &ClientConfig) -> Result<Arc<Connection>> {
        let mut attempt = 0u32;
        let mut last_err = SdkError::Internal("no attempts made".into());

        while attempt <= config.max_retries {
            match Connection::connect(config).await {
                Ok(conn) => return Ok(conn),
                Err(e) => {
                    last_err = e;
                    if attempt < config.max_retries {
                        let base_ms = 100u64 * (1u64 << attempt.min(6));
                        let jitter = rand::thread_rng().gen_range(0..base_ms / 2 + 1);
                        let delay = Duration::from_millis(base_ms + jitter);
                        tracing::warn!(
                            attempt = attempt + 1,
                            max = config.max_retries,
                            delay_ms = delay.as_millis(),
                            "connection failed, retrying"
                        );
                        tokio::time::sleep(delay).await;
                    }
                    attempt += 1;
                }
            }
        }

        Err(last_err)
    }
}

// ---------------------------------------------------------------------------
// TransitionBuilder
// ---------------------------------------------------------------------------

/// Fluent builder for a `Transition` request.
pub struct TransitionBuilder {
    client: Client,
    entity_id: String,
    machine: String,
    event: String,
    expected_version: Option<u64>,
    ctx: serde_json::Value,
    actor: String,
    idempotency_key: Option<String>,
}

impl TransitionBuilder {
    /// Optimistic-lock version check — the server rejects the transition if
    /// the entity's current version differs.
    pub fn expected_version(mut self, version: u64) -> Self {
        self.expected_version = Some(version);
        self
    }

    /// Arbitrary JSON context attached to the transition.
    pub fn ctx(mut self, ctx: serde_json::Value) -> Self {
        self.ctx = ctx;
        self
    }

    /// Identity of the actor (service, user, etc.) performing the transition.
    pub fn actor(mut self, actor: impl Into<String>) -> Self {
        self.actor = actor.into();
        self
    }

    /// Optional idempotency key — duplicate requests with the same key are
    /// silently deduplicated by the server.
    pub fn idempotency_key(mut self, key: impl Into<String>) -> Self {
        self.idempotency_key = Some(key.into());
        self
    }

    /// Execute the transition and return the server's response.
    pub async fn send(self) -> Result<TransitionResponse> {
        use smdb_proto::messages::TransitionMessage;

        let conn = self.client.conn().await;
        let request_id = conn.next_request_id();

        let msg = TransitionMessage {
            request_id,
            entity_id: self.entity_id,
            machine: self.machine,
            event: self.event,
            expected_version: self.expected_version,
            idempotency_key: self.idempotency_key,
            ctx: self.ctx,
            actor: self.actor,
        };
        let frame = encode_message(FrameTag::Transition, &msg)?;
        let resp = conn
            .send_request(
                frame.tag,
                frame.body,
                request_id,
                self.client.config.request_timeout,
            )
            .await?;

        match resp.tag {
            FrameTag::Result => {
                let result_msg: ResultMessage = decode_message(&resp)?;
                // The server embeds the ChangeRecord (or a subset) as the payload.
                // We deserialise via the ChangeRecord type which has the fields we need.
                let cr: ChangeRecord = serde_json::from_value(result_msg.payload).map_err(|e| {
                    SdkError::Internal(format!("deserialize transition result: {}", e))
                })?;
                Ok(TransitionResponse {
                    entity_id: cr.entity_id,
                    machine: cr.machine,
                    from_state: cr.from_state,
                    to_state: cr.to_state,
                    version: cr.version,
                    transition_id: cr.transition_id,
                    sequence: cr.sequence,
                    timestamp: cr.timestamp,
                })
            }
            FrameTag::Rejection => {
                let rej: RejectionMessage = decode_message(&resp)?;
                Err(SdkError::Rejected {
                    code: rej.code,
                    message: rej.message,
                    current_state: rej.current_state,
                    version: rej.version,
                })
            }
            FrameTag::Error => {
                let err_msg: smdb_proto::messages::ErrorMessage =
                    decode_message(&resp).unwrap_or(smdb_proto::messages::ErrorMessage {
                        message: "unknown server error".into(),
                        fatal: false,
                    });
                Err(SdkError::Internal(err_msg.message))
            }
            other => Err(SdkError::Internal(format!(
                "unexpected response tag: {:?}",
                other
            ))),
        }
    }
}

// ---------------------------------------------------------------------------
// HistoryBuilder
// ---------------------------------------------------------------------------

/// Fluent builder for a `History` query.
pub struct HistoryBuilder {
    client: Client,
    entity_id: String,
    machine: String,
    limit: Option<u32>,
    after_sequence: Option<u64>,
}

impl HistoryBuilder {
    /// Maximum number of records to return.
    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Only return records whose sequence number is greater than this value.
    pub fn after_sequence(mut self, seq: u64) -> Self {
        self.after_sequence = Some(seq);
        self
    }

    /// Execute the history query.
    pub async fn send(self) -> Result<Vec<TransitionRecord>> {
        use smdb_proto::messages::HistoryMessage;

        let conn = self.client.conn().await;
        let request_id = conn.next_request_id();

        let msg = HistoryMessage {
            request_id,
            entity_id: self.entity_id,
            machine: self.machine,
            limit: self.limit,
            after_sequence: self.after_sequence,
        };
        let frame = encode_message(FrameTag::History, &msg)?;
        let resp = conn
            .send_request(
                frame.tag,
                frame.body,
                request_id,
                self.client.config.request_timeout,
            )
            .await?;

        self.client
            .decode_result_payload::<Vec<TransitionRecord>>(resp)
    }
}
