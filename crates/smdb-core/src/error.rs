use thiserror::Error;

use crate::types::{EntityId, EventName, MachineName, GuardName, StateName, Version};

#[derive(Debug, Clone, Error)]
pub enum CoreError {
    #[error("illegal transition: entity '{entity_id}' in machine '{machine}' has no transition for event '{event}' from state '{current_state}'")]
    IllegalTransition {
        entity_id: EntityId,
        machine: MachineName,
        event: EventName,
        current_state: StateName,
    },

    #[error("guard '{guard_name}' failed: {reason}")]
    GuardFailed {
        guard_name: GuardName,
        reason: String,
    },

    #[error("version conflict: entity '{entity_id}' in machine '{machine}' expected version {expected} but found {actual}")]
    VersionConflict {
        entity_id: EntityId,
        machine: MachineName,
        expected: Version,
        actual: Version,
    },

    #[error("unknown machine: '{name}'")]
    UnknownMachine { name: MachineName },

    #[error("unknown entity '{entity_id}' in machine '{machine}'")]
    UnknownEntity {
        entity_id: EntityId,
        machine: MachineName,
    },

    #[error("invalid machine definition: {reason}")]
    InvalidDefinition { reason: String },

    #[error("duplicate state: '{name}'")]
    DuplicateState { name: StateName },

    #[error("duplicate event: '{name}'")]
    DuplicateEvent { name: EventName },
}

pub type Result<T> = std::result::Result<T, CoreError>;
