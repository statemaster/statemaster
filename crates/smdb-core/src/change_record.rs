use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::types::{
    ActorId, Context, EffectName, EntityId, EventName, MachineName, Sequence, StateName,
    TransitionId, Version,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EffectPayload {
    pub effect_name: EffectName,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeRecord {
    pub sequence: Sequence,
    pub transition_id: TransitionId,
    pub entity_id: EntityId,
    pub machine: MachineName,
    pub from_state: StateName,
    pub to_state: StateName,
    pub event: EventName,
    pub actor: ActorId,
    pub version: Version,
    pub timestamp: DateTime<Utc>,
    pub ctx: Context,
    pub effects: Vec<EffectPayload>,
}
