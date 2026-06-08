use std::collections::HashMap;

use chrono::{DateTime, Utc};
use smdb_core::prelude::ChangeRecord;
use tokio::sync::mpsc;

/// Represents one authenticated client connection.
pub struct Session {
    /// Unique session identifier (UUID).
    pub id: String,

    /// Whether this session has successfully authenticated.
    pub authenticated: bool,

    /// Capabilities advertised by the client during startup.
    pub capabilities: Vec<String>,

    /// Active subscriptions keyed by subscription_id.
    /// Each value is the receiver half of the change-record channel for that subscription.
    pub subscriptions: HashMap<String, mpsc::UnboundedReceiver<ChangeRecord>>,

    /// When this session was established.
    pub created_at: DateTime<Utc>,
}

impl Session {
    pub fn new(id: String) -> Self {
        Self {
            id,
            authenticated: false,
            capabilities: Vec::new(),
            subscriptions: HashMap::new(),
            created_at: Utc::now(),
        }
    }
}
