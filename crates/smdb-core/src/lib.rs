pub mod change_record;
pub mod effect;
pub mod engine;
pub mod entity;
pub mod error;
pub mod machine;
pub mod transition;
pub mod types;

pub mod prelude {
    pub use crate::change_record::{ChangeRecord, EffectPayload};
    pub use crate::effect::{Effect, EffectStatus};
    pub use crate::engine::FsmPlanner;
    pub use crate::entity::EntityState;
    pub use crate::error::{CoreError, Result};
    pub use crate::machine::{EffectRule, MachineBuilder, MachineDefinition, TransitionRule};
    pub use crate::transition::TransitionRecord;
    pub use crate::types::{
        ActorId, Context, EffectName, EntityId, EventName, GuardName, IdempotencyKey, MachineName,
        Sequence, StateName, TransitionId, Version,
    };
}
