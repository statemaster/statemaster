use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::types::{EntityId, MachineName, StateName, Version};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityState {
    pub entity_id: EntityId,
    pub machine: MachineName,
    pub current_state: StateName,
    pub version: Version,
    pub updated_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}
