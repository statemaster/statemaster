use serde::{Deserialize, Serialize};
use smdb_core::prelude::{ChangeRecord, MachineDefinition};

// --- Client → Server ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartupMessage {
    pub protocol_version: u32,
    pub capabilities: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthMessage {
    pub token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DefineMachineMessage {
    pub request_id: u64,
    pub name: String,
    pub version: u32,
    pub definition: MachineDefinition,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransitionMessage {
    pub request_id: u64,
    pub entity_id: String,
    pub machine: String,
    pub event: String,
    pub expected_version: Option<u64>,
    pub idempotency_key: Option<String>,
    pub ctx: serde_json::Value,
    pub actor: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CurrentMessage {
    pub request_id: u64,
    pub entity_id: String,
    pub machine: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryMessage {
    pub request_id: u64,
    pub entity_id: String,
    pub machine: String,
    pub limit: Option<u32>,
    pub after_sequence: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribeMessage {
    pub request_id: u64,
    pub subscription_id: String,
    pub machine_filter: Option<String>,
    pub after_sequence: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AckMessage {
    pub subscription_id: String,
    pub up_to_sequence: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnsubscribeMessage {
    pub subscription_id: String,
}

// --- Server → Client ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadyMessage {
    pub session_id: String,
    pub server_version: String,
    pub protocol_version: u32,
    pub capabilities: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultMessage {
    pub request_id: u64,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RejectionMessage {
    pub request_id: u64,
    /// One of: "illegal_transition", "guard_failed", "version_conflict",
    /// "unknown_machine", "unknown_entity"
    pub code: String,
    pub message: String,
    pub current_state: Option<String>,
    pub version: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeRecordMessage {
    pub subscription_id: String,
    pub record: ChangeRecord,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoticeMessage {
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorMessage {
    pub message: String,
    pub fatal: bool,
}
