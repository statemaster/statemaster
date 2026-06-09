//! `RedbEngine` — embedded redb-backed `StorageEngine` implementation.
//!
//! Table layout
//! ─────────────────────────────────────────────────────────────────
//!  machines           key: machine_name (str)          val: msgpack(MachineDefinition)
//!  entity_states      key: "entity_id\x00machine" (str) val: msgpack(EntityState)
//!  transitions        key: global_sequence (u64)        val: msgpack(TransitionRecord)
//!  transitions_by_id  key: transition_id (str)          val: global_sequence (u64)
//!  entity_transitions key: "entity_id\x00machine\x00seq" (str) val: global_sequence (u64)
//!  effects            key: effect_id (str)              val: msgpack(Effect)
//!  effects_by_txn     key: "transition_id\x00effect_id" val: effect_id (str)
//!  idempotency        key: key (str)                    val: global_sequence (u64)
//!  meta               key: "sequence" (str)             val: current_sequence (u64)

use std::path::Path;

use parking_lot::Mutex;
use redb::{Database, ReadableTable, TableDefinition};
use smdb_core::prelude::{
    Effect, EffectStatus, EntityState, MachineDefinition, Sequence, TransitionRecord,
};

use crate::engine::StorageEngine;
use crate::error::{Result, StorageError};

const MACHINES: TableDefinition<&str, &[u8]> = TableDefinition::new("machines");
const ENTITY_STATES: TableDefinition<&str, &[u8]> = TableDefinition::new("entity_states");
const TRANSITIONS: TableDefinition<u64, &[u8]> = TableDefinition::new("transitions");
const TRANSITIONS_BY_ID: TableDefinition<&str, u64> = TableDefinition::new("transitions_by_id");
const ENTITY_TRANSITIONS: TableDefinition<&str, u64> = TableDefinition::new("entity_transitions");
const EFFECTS: TableDefinition<&str, &[u8]> = TableDefinition::new("effects");
const EFFECTS_BY_TXN: TableDefinition<&str, &str> = TableDefinition::new("effects_by_txn");
const IDEMPOTENCY: TableDefinition<&str, u64> = TableDefinition::new("idempotency");
const META: TableDefinition<&str, u64> = TableDefinition::new("meta");

const META_SEQUENCE: &str = "sequence";

/// An embedded redb storage engine. All methods use synchronous redb
/// transactions which are protected by an internal `Mutex` to serialise writes
/// while allowing concurrent reads where redb supports it.
pub struct RedbEngine {
    db: Database,
    /// Serialises write transactions so sequence numbers are monotonic and
    /// there are no write-write conflicts on the meta sequence counter.
    write_lock: Mutex<()>,
}

impl RedbEngine {
    /// Open (or create) a database at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let db = Database::create(path.as_ref())?;
        let engine = Self {
            db,
            write_lock: Mutex::new(()),
        };
        engine.ensure_tables()?;
        Ok(engine)
    }

    /// Open an in-memory database (useful for tests).
    pub fn in_memory() -> Result<Self> {
        let db = Database::builder().create_with_backend(redb::backends::InMemoryBackend::new())?;
        let engine = Self {
            db,
            write_lock: Mutex::new(()),
        };
        engine.ensure_tables()?;
        Ok(engine)
    }

    fn ensure_tables(&self) -> Result<()> {
        let tx = self.db.begin_write()?;
        tx.open_table(MACHINES)?;
        tx.open_table(ENTITY_STATES)?;
        tx.open_table(TRANSITIONS)?;
        tx.open_table(TRANSITIONS_BY_ID)?;
        tx.open_table(ENTITY_TRANSITIONS)?;
        tx.open_table(EFFECTS)?;
        tx.open_table(EFFECTS_BY_TXN)?;
        tx.open_table(IDEMPOTENCY)?;
        tx.open_table(META)?;
        tx.commit()?;
        Ok(())
    }

    fn entity_state_key(entity_id: &str, machine: &str) -> String {
        format!("{}\x00{}", entity_id, machine)
    }

    fn entity_transition_key(entity_id: &str, machine: &str, seq: u64) -> String {
        // Zero-pad to 20 digits for lexicographic ordering.
        format!("{}\x00{}\x00{:020}", entity_id, machine, seq)
    }

    fn entity_transition_prefix(entity_id: &str, machine: &str) -> String {
        format!("{}\x00{}\x00", entity_id, machine)
    }

    fn effect_by_txn_key(transition_id: &str, effect_id: &str) -> String {
        format!("{}\x00{}", transition_id, effect_id)
    }

    fn effect_by_txn_prefix(transition_id: &str) -> String {
        format!("{}\x00", transition_id)
    }

    fn next_sequence_in_tx(tx: &redb::WriteTransaction) -> std::result::Result<u64, StorageError> {
        let mut meta = tx.open_table(META)?;
        let current = meta.get(META_SEQUENCE)?.map(|v| v.value()).unwrap_or(0u64);
        let next = current + 1;
        meta.insert(META_SEQUENCE, next)?;
        Ok(next)
    }
}

impl StorageEngine for RedbEngine {
    // ── Machine definitions ──────────────────────────────────────────────────

    fn store_machine(&self, machine: &MachineDefinition) -> Result<()> {
        let _guard = self.write_lock.lock();
        let tx = self.db.begin_write()?;
        {
            let mut tbl = tx.open_table(MACHINES)?;
            let bytes = rmp_serde::to_vec(machine)?;
            tbl.insert(machine.name.as_str(), bytes.as_slice())?;
        }
        tx.commit()?;
        Ok(())
    }

    fn get_machine(&self, name: &str, _version: Option<u32>) -> Result<MachineDefinition> {
        let tx = self.db.begin_read()?;
        let tbl = tx.open_table(MACHINES)?;
        match tbl.get(name)? {
            Some(v) => {
                let def: MachineDefinition = rmp_serde::from_slice(v.value())?;
                Ok(def)
            }
            None => Err(StorageError::NotFound(format!("machine '{}'", name))),
        }
    }

    fn list_machines(&self) -> Result<Vec<MachineDefinition>> {
        let tx = self.db.begin_read()?;
        let tbl = tx.open_table(MACHINES)?;
        let mut results = Vec::new();
        for entry in tbl.iter()? {
            let (_, v) = entry?;
            let def: MachineDefinition = rmp_serde::from_slice(v.value())?;
            results.push(def);
        }
        Ok(results)
    }

    // ── Entity state ─────────────────────────────────────────────────────────

    fn get_entity_state(&self, entity_id: &str, machine: &str) -> Result<EntityState> {
        let key = Self::entity_state_key(entity_id, machine);
        let tx = self.db.begin_read()?;
        let tbl = tx.open_table(ENTITY_STATES)?;
        match tbl.get(key.as_str())? {
            Some(v) => {
                let state: EntityState = rmp_serde::from_slice(v.value())?;
                Ok(state)
            }
            None => Err(StorageError::NotFound(format!(
                "entity '{}' in machine '{}'",
                entity_id, machine
            ))),
        }
    }

    fn upsert_entity_state(&self, state: &EntityState) -> Result<()> {
        let _guard = self.write_lock.lock();
        let tx = self.db.begin_write()?;
        {
            let key = Self::entity_state_key(&state.entity_id, &state.machine);
            let mut tbl = tx.open_table(ENTITY_STATES)?;

            // Optimistic concurrency check.
            if let Some(existing) = tbl.get(key.as_str())? {
                let existing: EntityState = rmp_serde::from_slice(existing.value())?;
                if state.version > 1 && existing.version != state.version - 1 {
                    return Err(StorageError::VersionConflict {
                        entity_id: state.entity_id.clone(),
                        expected: state.version - 1,
                        actual: existing.version,
                    });
                }
            }

            let bytes = rmp_serde::to_vec(state)?;
            tbl.insert(key.as_str(), bytes.as_slice())?;
        }
        tx.commit()?;
        Ok(())
    }

    fn list_entities_in_state(&self, machine: &str, state_name: &str) -> Result<Vec<EntityState>> {
        let tx = self.db.begin_read()?;
        let tbl = tx.open_table(ENTITY_STATES)?;
        let mut results = Vec::new();
        for entry in tbl.iter()? {
            let (k, v) = entry?;
            // Only include entries belonging to this machine.
            if k.value().contains(&format!("\x00{}", machine) as &str) {
                let state: EntityState = rmp_serde::from_slice(v.value())?;
                if state.machine == machine && state.current_state == state_name {
                    results.push(state);
                }
            }
        }
        Ok(results)
    }

    // ── Transition log ───────────────────────────────────────────────────────

    fn append_transition(&self, record: &mut TransitionRecord) -> Result<Sequence> {
        let _guard = self.write_lock.lock();
        let tx = self.db.begin_write()?;
        let seq = Self::next_sequence_in_tx(&tx)?;
        {
            record.sequence = seq;
            let bytes = rmp_serde::to_vec(&*record)?;
            let mut tbl = tx.open_table(TRANSITIONS)?;
            tbl.insert(seq, bytes.as_slice())?;

            let mut by_id = tx.open_table(TRANSITIONS_BY_ID)?;
            by_id.insert(record.id.as_str(), seq)?;

            let et_key = Self::entity_transition_key(&record.entity_id, &record.machine, seq);
            let mut entity_tbl = tx.open_table(ENTITY_TRANSITIONS)?;
            entity_tbl.insert(et_key.as_str(), seq)?;
        }
        tx.commit()?;
        Ok(seq)
    }

    fn get_transition(&self, id: &str) -> Result<TransitionRecord> {
        let tx = self.db.begin_read()?;
        let by_id = tx.open_table(TRANSITIONS_BY_ID)?;
        let seq = by_id
            .get(id)?
            .ok_or_else(|| StorageError::NotFound(format!("transition '{}'", id)))?
            .value();
        let tbl = tx.open_table(TRANSITIONS)?;
        let record: TransitionRecord =
            tbl.get(seq)?
                .map(|v| rmp_serde::from_slice(v.value()))
                .ok_or_else(|| StorageError::NotFound(format!("transition seq {}", seq)))??;
        Ok(record)
    }

    fn get_history(
        &self,
        entity_id: &str,
        machine: &str,
        limit: Option<u32>,
        after_sequence: Option<Sequence>,
    ) -> Result<Vec<TransitionRecord>> {
        let prefix = Self::entity_transition_prefix(entity_id, machine);
        let tx = self.db.begin_read()?;
        let entity_tbl = tx.open_table(ENTITY_TRANSITIONS)?;
        let trans_tbl = tx.open_table(TRANSITIONS)?;
        let limit = limit.unwrap_or(u32::MAX) as usize;
        let after = after_sequence.unwrap_or(0);
        let mut results = Vec::new();

        for entry in entity_tbl.iter()? {
            let (k, seq_val) = entry?;
            let key = k.value();
            if !key.starts_with(prefix.as_str()) {
                continue;
            }
            let seq = seq_val.value();
            if seq <= after {
                continue;
            }
            if results.len() >= limit {
                break;
            }
            if let Some(bytes) = trans_tbl.get(seq)? {
                let record: TransitionRecord = rmp_serde::from_slice(bytes.value())?;
                results.push(record);
            }
        }

        Ok(results)
    }

    fn get_transitions_after(
        &self,
        after_sequence: Sequence,
        limit: u32,
    ) -> Result<Vec<TransitionRecord>> {
        let tx = self.db.begin_read()?;
        let tbl = tx.open_table(TRANSITIONS)?;
        let mut results = Vec::new();
        let start = after_sequence + 1;

        for entry in tbl.range(start..)? {
            if results.len() >= limit as usize {
                break;
            }
            let (_, v) = entry?;
            let record: TransitionRecord = rmp_serde::from_slice(v.value())?;
            results.push(record);
        }

        Ok(results)
    }

    // ── Outbox ───────────────────────────────────────────────────────────────

    fn insert_effects(&self, effects: &[Effect]) -> Result<()> {
        let _guard = self.write_lock.lock();
        let tx = self.db.begin_write()?;
        {
            let mut tbl = tx.open_table(EFFECTS)?;
            let mut by_txn = tx.open_table(EFFECTS_BY_TXN)?;
            for effect in effects {
                let bytes = rmp_serde::to_vec(effect)?;
                tbl.insert(effect.id.as_str(), bytes.as_slice())?;
                let key = Self::effect_by_txn_key(&effect.transition_id, &effect.id);
                by_txn.insert(key.as_str(), effect.id.as_str())?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    fn get_pending_effects(&self, limit: u32) -> Result<Vec<Effect>> {
        let tx = self.db.begin_read()?;
        let tbl = tx.open_table(EFFECTS)?;
        let mut results = Vec::new();

        for entry in tbl.iter()? {
            if results.len() >= limit as usize {
                break;
            }
            let (_, v) = entry?;
            let effect: Effect = rmp_serde::from_slice(v.value())?;
            if effect.status == EffectStatus::Pending {
                results.push(effect);
            }
        }

        Ok(results)
    }

    fn get_effects_for_transition(&self, transition_id: &str) -> Result<Vec<Effect>> {
        let prefix = Self::effect_by_txn_prefix(transition_id);
        let tx = self.db.begin_read()?;
        let by_txn = tx.open_table(EFFECTS_BY_TXN)?;
        let effects_tbl = tx.open_table(EFFECTS)?;
        let mut results = Vec::new();
        for entry in by_txn.range(prefix.as_str()..)? {
            let (k, v) = entry?;
            if !k.value().starts_with(prefix.as_str()) {
                break;
            }
            if let Some(bytes) = effects_tbl.get(v.value())? {
                results.push(rmp_serde::from_slice(bytes.value())?);
            }
        }
        Ok(results)
    }

    fn mark_effect_published(&self, effect_id: &str) -> Result<()> {
        self.update_effect_status(effect_id, EffectStatus::Published)
    }

    fn mark_effect_failed(&self, effect_id: &str) -> Result<()> {
        self.update_effect_status(effect_id, EffectStatus::Failed)
    }

    // ── Idempotency ──────────────────────────────────────────────────────────

    fn check_idempotency(&self, key: &str) -> Result<Option<TransitionRecord>> {
        let tx = self.db.begin_read()?;
        let idem = tx.open_table(IDEMPOTENCY)?;
        let seq = match idem.get(key)? {
            Some(v) => v.value(),
            None => return Ok(None),
        };
        let tbl = tx.open_table(TRANSITIONS)?;
        let record: TransitionRecord =
            tbl.get(seq)?
                .map(|v| rmp_serde::from_slice(v.value()))
                .ok_or_else(|| StorageError::NotFound(format!("transition seq {}", seq)))??;
        Ok(Some(record))
    }

    // ── Atomic transition ────────────────────────────────────────────────────

    fn execute_transition(
        &self,
        record: &mut TransitionRecord,
        new_state: &EntityState,
        effects: &[Effect],
    ) -> Result<Sequence> {
        let _guard = self.write_lock.lock();
        let tx = self.db.begin_write()?;

        // 1. Assign sequence and write transition log entry.
        let seq = Self::next_sequence_in_tx(&tx)?;
        record.sequence = seq;

        {
            let bytes = rmp_serde::to_vec(&*record)?;
            let mut tbl = tx.open_table(TRANSITIONS)?;
            tbl.insert(seq, bytes.as_slice())?;

            let mut by_id = tx.open_table(TRANSITIONS_BY_ID)?;
            by_id.insert(record.id.as_str(), seq)?;

            let et_key = Self::entity_transition_key(&record.entity_id, &record.machine, seq);
            let mut entity_tbl = tx.open_table(ENTITY_TRANSITIONS)?;
            entity_tbl.insert(et_key.as_str(), seq)?;
        }

        // 2. Upsert entity state projection.
        {
            let key = Self::entity_state_key(&new_state.entity_id, &new_state.machine);
            let mut tbl = tx.open_table(ENTITY_STATES)?;

            // Optimistic concurrency check (version already set by caller).
            if let Some(existing_bytes) = tbl.get(key.as_str())? {
                let existing: EntityState = rmp_serde::from_slice(existing_bytes.value())?;
                let expected_prev = new_state.version.saturating_sub(1);
                if existing.version != expected_prev {
                    return Err(StorageError::VersionConflict {
                        entity_id: new_state.entity_id.clone(),
                        expected: expected_prev,
                        actual: existing.version,
                    });
                }
            }

            let state_bytes = rmp_serde::to_vec(new_state)?;
            tbl.insert(key.as_str(), state_bytes.as_slice())?;
        }

        // 3. Insert effects into the outbox (+ the by-transition index used to
        //    reconstruct change records during stream delivery).
        {
            let mut tbl = tx.open_table(EFFECTS)?;
            let mut by_txn = tx.open_table(EFFECTS_BY_TXN)?;
            for effect in effects {
                let bytes = rmp_serde::to_vec(effect)?;
                tbl.insert(effect.id.as_str(), bytes.as_slice())?;
                let key = Self::effect_by_txn_key(&effect.transition_id, &effect.id);
                by_txn.insert(key.as_str(), effect.id.as_str())?;
            }
        }

        // 4. Store idempotency key → sequence.
        if let Some(ref idem_key) = record.idempotency_key {
            let mut tbl = tx.open_table(IDEMPOTENCY)?;
            // If the key is already there we consider it a conflict.
            if tbl.get(idem_key.as_str())?.is_some() {
                return Err(StorageError::AlreadyExists(format!(
                    "idempotency key '{}'",
                    idem_key
                )));
            }
            tbl.insert(idem_key.as_str(), seq)?;
        }

        tx.commit()?;
        Ok(seq)
    }

    // ── Sequence counter ─────────────────────────────────────────────────────

    fn current_sequence(&self) -> Result<Sequence> {
        let tx = self.db.begin_read()?;
        let tbl = tx.open_table(META)?;
        let seq = tbl.get(META_SEQUENCE)?.map(|v| v.value()).unwrap_or(0u64);
        Ok(seq)
    }
}

impl RedbEngine {
    fn update_effect_status(&self, effect_id: &str, status: EffectStatus) -> Result<()> {
        let _guard = self.write_lock.lock();
        let tx = self.db.begin_write()?;
        {
            let mut tbl = tx.open_table(EFFECTS)?;
            let bytes = tbl
                .get(effect_id)?
                .ok_or_else(|| StorageError::NotFound(format!("effect '{}'", effect_id)))?
                .value()
                .to_vec();
            let mut effect: Effect = rmp_serde::from_slice(&bytes)?;
            effect.status = status;
            let updated = rmp_serde::to_vec(&effect)?;
            tbl.insert(effect_id, updated.as_slice())?;
        }
        tx.commit()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use smdb_core::prelude::{Effect, FsmPlanner, MachineBuilder};

    use super::*;

    fn build_fulfillment() -> MachineDefinition {
        MachineBuilder::new()
            .name("fulfillment")
            .version(1)
            .states(["pending", "paid", "shipped", "delivered"])
            .initial_state("pending")
            .transition("pay", ["pending"], "paid")
            .transition("ship", ["paid"], "shipped")
            .transition("deliver", ["shipped"], "delivered")
            .build()
            .unwrap()
    }

    #[test]
    fn store_and_get_machine() {
        let engine = RedbEngine::in_memory().unwrap();
        let def = build_fulfillment();
        engine.store_machine(&def).unwrap();
        let got = engine.get_machine("fulfillment", None).unwrap();
        assert_eq!(got.name, "fulfillment");
    }

    #[test]
    fn list_machines_returns_all() {
        let engine = RedbEngine::in_memory().unwrap();
        engine.store_machine(&build_fulfillment()).unwrap();
        let list = engine.list_machines().unwrap();
        assert_eq!(list.len(), 1);
    }

    #[test]
    fn execute_transition_and_read_state() {
        let engine = RedbEngine::in_memory().unwrap();
        let def = build_fulfillment();
        engine.store_machine(&def).unwrap();

        let now = chrono::Utc::now();
        let new_state = EntityState {
            entity_id: "e1".to_string(),
            machine: "fulfillment".to_string(),
            current_state: "paid".to_string(),
            version: 1,
            updated_at: now,
            created_at: now,
        };
        let mut record = FsmPlanner::build_transition_record(
            "e1".into(),
            "fulfillment".into(),
            "pending".into(),
            "paid".into(),
            "pay".into(),
            "actor".into(),
            serde_json::json!({}),
            None,
        );

        let seq = engine
            .execute_transition(&mut record, &new_state, &[])
            .unwrap();
        assert_eq!(seq, 1);
        assert_eq!(record.sequence, 1);

        let state = engine.get_entity_state("e1", "fulfillment").unwrap();
        assert_eq!(state.current_state, "paid");
        assert_eq!(state.version, 1);
    }

    #[test]
    fn idempotency_key_prevents_duplicate() {
        let engine = RedbEngine::in_memory().unwrap();

        let now = chrono::Utc::now();
        let new_state = EntityState {
            entity_id: "e1".to_string(),
            machine: "fulfillment".to_string(),
            current_state: "paid".to_string(),
            version: 1,
            updated_at: now,
            created_at: now,
        };
        let mut record = FsmPlanner::build_transition_record(
            "e1".into(),
            "fulfillment".into(),
            "pending".into(),
            "paid".into(),
            "pay".into(),
            "actor".into(),
            serde_json::json!({}),
            Some("idem-1".into()),
        );

        engine
            .execute_transition(&mut record, &new_state, &[])
            .unwrap();

        // Second attempt with same idempotency key must fail.
        let mut record2 = FsmPlanner::build_transition_record(
            "e1".into(),
            "fulfillment".into(),
            "pending".into(),
            "paid".into(),
            "pay".into(),
            "actor".into(),
            serde_json::json!({}),
            Some("idem-1".into()),
        );
        let new_state2 = EntityState {
            entity_id: "e1".to_string(),
            machine: "fulfillment".to_string(),
            current_state: "paid".to_string(),
            version: 2,
            updated_at: now,
            created_at: now,
        };
        let result = engine.execute_transition(&mut record2, &new_state2, &[]);
        assert!(matches!(result, Err(StorageError::AlreadyExists(_))));
    }

    #[test]
    fn effect_lifecycle() {
        let engine = RedbEngine::in_memory().unwrap();
        let effect = Effect::new(
            "tr1".into(),
            "notify_customer".into(),
            serde_json::json!({}),
        );
        let id = effect.id.clone();
        engine.insert_effects(&[effect]).unwrap();

        let pending = engine.get_pending_effects(10).unwrap();
        assert_eq!(pending.len(), 1);

        engine.mark_effect_published(&id).unwrap();
        let still_pending = engine.get_pending_effects(10).unwrap();
        assert!(still_pending.is_empty());
    }

    #[test]
    fn get_history_returns_ordered_records() {
        let engine = RedbEngine::in_memory().unwrap();
        let now = chrono::Utc::now();

        for (from, to, event, version) in [
            ("pending", "paid", "pay", 1u64),
            ("paid", "shipped", "ship", 2u64),
        ] {
            let new_state = EntityState {
                entity_id: "e1".to_string(),
                machine: "fulfillment".to_string(),
                current_state: to.to_string(),
                version,
                updated_at: now,
                created_at: now,
            };
            let mut record = FsmPlanner::build_transition_record(
                "e1".into(),
                "fulfillment".into(),
                from.into(),
                to.into(),
                event.into(),
                "actor".into(),
                serde_json::json!({}),
                None,
            );
            engine
                .execute_transition(&mut record, &new_state, &[])
                .unwrap();
        }

        let history = engine.get_history("e1", "fulfillment", None, None).unwrap();
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].event, "pay");
        assert_eq!(history[1].event, "ship");
    }
}
