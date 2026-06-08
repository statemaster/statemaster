use chrono::{DateTime, Utc};

/// Returned by [`TransitionBuilder::send()`] on a successful state transition.
#[derive(Debug, Clone)]
pub struct TransitionResponse {
    pub entity_id: String,
    pub machine: String,
    pub from_state: String,
    pub to_state: String,
    pub version: u64,
    pub transition_id: String,
    pub sequence: u64,
    pub timestamp: DateTime<Utc>,
}
