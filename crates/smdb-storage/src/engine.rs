use smdb_core::prelude::{Effect, EntityState, MachineDefinition, Sequence, TransitionRecord};

use crate::error::Result;

/// The core storage abstraction for StateMaster. v1 uses a `redb`-backed
/// embedded implementation; future versions can swap in a custom engine by
/// providing a different `impl StorageEngine`.
pub trait StorageEngine: Send + Sync {
    // -------------------------------------------------------------------------
    // Machine definitions
    // -------------------------------------------------------------------------

    /// Persist a machine definition. If a definition with the same
    /// `(name, version)` pair already exists the call returns
    /// `StorageError::AlreadyExists`.
    fn store_machine(&self, machine: &MachineDefinition) -> Result<()>;

    /// Retrieve a machine definition by name and optional version. When
    /// `version` is `None` the latest version is returned.
    fn get_machine(&self, name: &str, version: Option<u32>) -> Result<MachineDefinition>;

    /// List all machine definitions (one entry per unique `(name, version)`).
    fn list_machines(&self) -> Result<Vec<MachineDefinition>>;

    // -------------------------------------------------------------------------
    // Entity state (the materialised projection)
    // -------------------------------------------------------------------------

    /// Return the current projected state for an entity in a named machine.
    fn get_entity_state(&self, entity_id: &str, machine: &str) -> Result<EntityState>;

    /// Insert or update the projected state for an entity. Performs optimistic
    /// concurrency checking: if a record already exists its `version` must
    /// equal `state.version - 1`, otherwise `StorageError::VersionConflict` is
    /// returned.
    fn upsert_entity_state(&self, state: &EntityState) -> Result<()>;

    /// Return all entities that are currently in `state_name` within `machine`.
    fn list_entities_in_state(
        &self,
        machine: &str,
        state_name: &str,
    ) -> Result<Vec<EntityState>>;

    // -------------------------------------------------------------------------
    // Transition log (append-only, source of truth)
    // -------------------------------------------------------------------------

    /// Append a transition to the log. The storage engine assigns the global
    /// sequence number and writes it back into `record.sequence`.
    fn append_transition(&self, record: &mut TransitionRecord) -> Result<Sequence>;

    /// Retrieve a single transition record by its UUID string id.
    fn get_transition(&self, id: &str) -> Result<TransitionRecord>;

    /// Return the ordered history of transitions for an entity within a
    /// machine. `limit` caps the number of results; `after_sequence` acts as a
    /// pagination cursor (exclusive).
    fn get_history(
        &self,
        entity_id: &str,
        machine: &str,
        limit: Option<u32>,
        after_sequence: Option<Sequence>,
    ) -> Result<Vec<TransitionRecord>>;

    /// Return transitions with a global sequence number strictly greater than
    /// `after_sequence`, up to `limit` entries, ordered by sequence ascending.
    /// Used for CDC / replication consumers.
    fn get_transitions_after(
        &self,
        after_sequence: Sequence,
        limit: u32,
    ) -> Result<Vec<TransitionRecord>>;

    // -------------------------------------------------------------------------
    // Outbox (effects awaiting publication)
    // -------------------------------------------------------------------------

    /// Insert one or more effects into the outbox with `EffectStatus::Pending`.
    fn insert_effects(&self, effects: &[Effect]) -> Result<()>;

    /// Return up to `limit` pending effects ordered by `created_at` ascending.
    fn get_pending_effects(&self, limit: u32) -> Result<Vec<Effect>>;

    /// Return all effects emitted by a single transition, in insertion order.
    /// Used to reconstruct a `ChangeRecord` from the log during stream delivery.
    fn get_effects_for_transition(&self, transition_id: &str) -> Result<Vec<Effect>>;

    /// Mark an outbox entry as successfully published.
    fn mark_effect_published(&self, effect_id: &str) -> Result<()>;

    /// Mark an outbox entry as failed (delivery could not be guaranteed).
    fn mark_effect_failed(&self, effect_id: &str) -> Result<()>;

    // -------------------------------------------------------------------------
    // Idempotency
    // -------------------------------------------------------------------------

    /// Look up whether a prior `TransitionRecord` was committed under the
    /// given idempotency key. Returns `None` if the key has not been seen.
    fn check_idempotency(&self, key: &str) -> Result<Option<TransitionRecord>>;

    // -------------------------------------------------------------------------
    // Atomic transition
    // -------------------------------------------------------------------------

    /// Atomically, in a single write transaction:
    /// 1. Assign the next global sequence and write `record` to the transition
    ///    log.
    /// 2. Upsert `new_state` into the entity state projection.
    /// 3. Insert all `effects` into the outbox.
    /// 4. Store `record.idempotency_key` (if any) in the idempotency table.
    ///
    /// This is the preferred write path; all other individual write methods are
    /// provided for tooling and back-fill scenarios.
    fn execute_transition(
        &self,
        record: &mut TransitionRecord,
        new_state: &EntityState,
        effects: &[Effect],
    ) -> Result<Sequence>;

    // -------------------------------------------------------------------------
    // Sequence counter
    // -------------------------------------------------------------------------

    /// Return the current (last assigned) global sequence number.
    fn current_sequence(&self) -> Result<Sequence>;
}
