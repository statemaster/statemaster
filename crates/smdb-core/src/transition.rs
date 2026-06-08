use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::types::{
    ActorId, Context, EntityId, EventName, IdempotencyKey, MachineName, Sequence, StateName,
    TransitionId,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransitionRecord {
    pub id: TransitionId,
    /// 0 means unassigned; the storage layer sets this when persisting.
    pub sequence: Sequence,
    pub entity_id: EntityId,
    pub machine: MachineName,
    pub from_state: StateName,
    pub to_state: StateName,
    pub event: EventName,
    pub actor: ActorId,
    pub ctx: Context,
    pub idempotency_key: Option<IdempotencyKey>,
    pub timestamp: DateTime<Utc>,
}
