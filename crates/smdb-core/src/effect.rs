use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::types::{EffectName, TransitionId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectStatus {
    Pending,
    Published,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Effect {
    pub id: String,
    pub transition_id: TransitionId,
    pub effect_name: EffectName,
    pub payload: serde_json::Value,
    pub status: EffectStatus,
    pub created_at: DateTime<Utc>,
}

impl Effect {
    pub fn new(
        transition_id: TransitionId,
        effect_name: EffectName,
        payload: serde_json::Value,
    ) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            transition_id,
            effect_name,
            payload,
            status: EffectStatus::Pending,
            created_at: Utc::now(),
        }
    }
}
