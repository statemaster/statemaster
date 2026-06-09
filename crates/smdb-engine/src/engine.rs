use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, watch, Notify};

/// Number of entity lock stripes. Transitions on the same `(entity, machine)`
/// always hash to the same stripe and therefore serialise; distinct entities
/// only contend on hash collision. Fixed size keeps memory bounded regardless
/// of how many distinct entities are seen.
const ENTITY_LOCK_STRIPES: usize = 1024;

use smdb_core::prelude::*;
use smdb_storage::StorageEngine;

use crate::error::{EngineError, Result};
use crate::guard::GuardRegistry;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransitionResult {
    pub entity_id: String,
    pub machine: String,
    pub from_state: String,
    pub to_state: String,
    pub version: Version,
    pub transition_id: String,
    pub sequence: Sequence,
    pub timestamp: DateTime<Utc>,
}

struct Subscriber {
    id: String,
    machine_filter: Option<String>,
    sender: mpsc::UnboundedSender<ChangeRecord>,
    /// Highest sequence already delivered to this subscriber. The dispatcher is
    /// the sole writer to `sender`, so advancing this monotonically guarantees
    /// in-order, gap-free delivery (and at-least-once: a record is only skipped
    /// once it has been handed to the channel).
    delivered_through: AtomicU64,
}

pub struct Engine {
    storage: Arc<dyn StorageEngine>,
    guards: Arc<RwLock<GuardRegistry>>,
    /// Striped per-entity locks. A `parking_lot::Mutex` (not a tokio mutex) so
    /// it can be taken from a blocking context without panicking; the engine is
    /// synchronous and is driven from `spawn_blocking` by the wire layer.
    entity_locks: Vec<Mutex<()>>,
    subscribers: Arc<RwLock<Vec<Subscriber>>>,
    /// Signalled after every committed transition so the dispatcher can wake and
    /// fan new change records out to subscribers without waiting for its poll.
    commit_notify: Arc<Notify>,
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
}

impl Engine {
    pub fn new(storage: Arc<dyn StorageEngine>) -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        Self {
            storage,
            guards: Arc::new(RwLock::new(GuardRegistry::new())),
            entity_locks: (0..ENTITY_LOCK_STRIPES).map(|_| Mutex::new(())).collect(),
            subscribers: Arc::new(RwLock::new(Vec::new())),
            commit_notify: Arc::new(Notify::new()),
            shutdown_tx,
            shutdown_rx,
        }
    }

    /// A handle the dispatcher awaits to be woken on each commit.
    pub fn commit_notify(&self) -> Arc<Notify> {
        Arc::clone(&self.commit_notify)
    }

    /// Map an `(entity, machine)` pair to its lock stripe.
    fn entity_lock(&self, entity_id: &str, machine: &str) -> &Mutex<()> {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        entity_id.hash(&mut hasher);
        machine.hash(&mut hasher);
        let idx = (hasher.finish() as usize) % ENTITY_LOCK_STRIPES;
        &self.entity_locks[idx]
    }

    pub fn storage(&self) -> &Arc<dyn StorageEngine> {
        &self.storage
    }

    pub fn shutdown_rx(&self) -> watch::Receiver<bool> {
        self.shutdown_rx.clone()
    }

    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }

    pub fn register_guard(&self, name: &str, guard: crate::guard::GuardFn) {
        self.guards.write().register(name, guard);
    }

    pub fn define_machine(&self, definition: MachineDefinition) -> Result<()> {
        definition.validate().map_err(EngineError::Core)?;
        self.storage
            .store_machine(&definition)
            .map_err(EngineError::Storage)
    }

    pub fn get_machine(&self, name: &str) -> Result<MachineDefinition> {
        self.storage
            .get_machine(name, None)
            .map_err(EngineError::Storage)
    }

    pub fn list_machines(&self) -> Result<Vec<MachineDefinition>> {
        self.storage.list_machines().map_err(EngineError::Storage)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn transition(
        &self,
        entity_id: &str,
        machine_name: &str,
        event: &str,
        actor: &str,
        ctx: serde_json::Value,
        expected_version: Option<Version>,
        idempotency_key: Option<String>,
    ) -> Result<TransitionResult> {
        if *self.shutdown_rx.borrow() {
            return Err(EngineError::ShuttingDown);
        }

        // Serialise on the entity stripe *before* the idempotency check so that
        // two concurrent retries carrying the same key cannot both miss it and
        // race into a duplicate transition.
        let _guard = self.entity_lock(entity_id, machine_name).lock();

        if let Some(ref key) = idempotency_key {
            if let Ok(Some(existing)) = self.storage.check_idempotency(key) {
                return Ok(TransitionResult {
                    entity_id: existing.entity_id.clone(),
                    machine: existing.machine.clone(),
                    from_state: existing.from_state.clone(),
                    to_state: existing.to_state.clone(),
                    version: existing.version,
                    transition_id: existing.id.clone(),
                    sequence: existing.sequence,
                    timestamp: existing.timestamp,
                });
            }
        }

        let definition = self.storage.get_machine(machine_name, None)?;

        let now = Utc::now();
        let (current_state, current_version, created_at) =
            match self.storage.get_entity_state(entity_id, machine_name) {
                Ok(state) => (state.current_state.clone(), state.version, state.created_at),
                Err(smdb_storage::StorageError::NotFound(_)) => {
                    (definition.initial_state.clone(), 0, now)
                }
                Err(e) => return Err(EngineError::Storage(e)),
            };

        if let Some(expected) = expected_version {
            if expected != current_version {
                return Err(EngineError::Core(CoreError::VersionConflict {
                    entity_id: entity_id.to_string(),
                    machine: machine_name.to_string(),
                    expected,
                    actual: current_version,
                }));
            }
        }

        let rule = FsmPlanner::plan_transition(
            &definition,
            &current_state,
            &event.to_string(),
            &entity_id.to_string(),
        )?;

        let guard_results: HashMap<String, bool> = {
            let registry = self.guards.read();
            let dummy_state = EntityState {
                entity_id: entity_id.to_string(),
                machine: machine_name.to_string(),
                current_state: current_state.clone(),
                version: current_version,
                updated_at: Utc::now(),
                created_at: Utc::now(),
            };
            rule.guards
                .iter()
                .map(|g| (g.clone(), registry.evaluate(g, &dummy_state, &ctx)))
                .collect()
        };
        FsmPlanner::validate_guards(rule, &guard_results)?;

        let mut record = FsmPlanner::build_transition_record(
            entity_id.to_string(),
            machine_name.to_string(),
            current_state.clone(),
            rule.to_state.clone(),
            event.to_string(),
            actor.to_string(),
            ctx.clone(),
            idempotency_key,
        );

        let effect_rules = FsmPlanner::compute_effects(&definition, &event.to_string());
        let effects: Vec<Effect> = effect_rules
            .iter()
            .map(|er| {
                Effect::new(
                    record.id.clone(),
                    er.effect.clone(),
                    er.payload.clone().unwrap_or_default(),
                )
            })
            .collect();

        let new_version = current_version + 1;
        record.version = new_version;
        let new_state = EntityState {
            entity_id: entity_id.to_string(),
            machine: machine_name.to_string(),
            current_state: rule.to_state.clone(),
            version: new_version,
            updated_at: now,
            created_at,
        };

        let sequence = self
            .storage
            .execute_transition(&mut record, &new_state, &effects)?;

        // Delivery is handled exclusively by the dispatcher reading the log, so
        // the commit path just records durably and wakes it. This keeps a single
        // ordered delivery path and makes the stream replayable/at-least-once.
        self.commit_notify.notify_one();

        Ok(TransitionResult {
            entity_id: entity_id.to_string(),
            machine: machine_name.to_string(),
            from_state: current_state,
            to_state: rule.to_state.clone(),
            version: new_version,
            transition_id: record.id,
            sequence,
            timestamp: record.timestamp,
        })
    }

    pub fn current(&self, entity_id: &str, machine: &str) -> Result<EntityState> {
        self.storage
            .get_entity_state(entity_id, machine)
            .map_err(EngineError::Storage)
    }

    pub fn history(
        &self,
        entity_id: &str,
        machine: &str,
        limit: Option<u32>,
        after_sequence: Option<Sequence>,
    ) -> Result<Vec<TransitionRecord>> {
        self.storage
            .get_history(entity_id, machine, limit, after_sequence)
            .map_err(EngineError::Storage)
    }

    /// Register a subscriber. Both the initial backfill (everything after
    /// `after_sequence`) and the live tail are delivered uniformly by the
    /// dispatcher reading the log, so there is no separate backfill path and no
    /// ordering race between catch-up and live records.
    pub fn subscribe(
        &self,
        id: String,
        machine_filter: Option<String>,
        after_sequence: Sequence,
    ) -> Result<mpsc::UnboundedReceiver<ChangeRecord>> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.subscribers.write().push(Subscriber {
            id,
            machine_filter,
            sender: tx,
            delivered_through: AtomicU64::new(after_sequence),
        });
        // Wake the dispatcher so the new subscriber is backfilled promptly.
        self.commit_notify.notify_one();
        Ok(rx)
    }

    pub fn unsubscribe(&self, id: &str) {
        self.subscribers.write().retain(|s| s.id != id);
    }

    /// Deliver any not-yet-sent change records to every subscriber, advancing
    /// each subscriber's cursor. Called by the dispatcher (off the async
    /// reactor, since it does blocking storage reads). Returns the number of
    /// records delivered. Dead subscribers (receiver dropped) are pruned.
    pub fn dispatch_pass(&self) -> usize {
        const BATCH: u32 = 256;
        let mut delivered = 0usize;
        let mut dead: Vec<String> = Vec::new();

        {
            let subs = self.subscribers.read();
            for sub in subs.iter() {
                let mut cursor = sub.delivered_through.load(Ordering::Acquire);
                'drain: while let Ok(records) = self.storage.get_transitions_after(cursor, BATCH) {
                    if records.is_empty() {
                        break;
                    }
                    let batch_len = records.len();
                    for rec in &records {
                        // Advance past every record we inspect, even filtered-out
                        // ones, so they are not re-scanned next pass.
                        cursor = rec.sequence;
                        if let Some(ref filter) = sub.machine_filter {
                            if &rec.machine != filter {
                                continue;
                            }
                        }
                        let effects = self
                            .storage
                            .get_effects_for_transition(&rec.id)
                            .unwrap_or_default();
                        let change = ChangeRecord {
                            sequence: rec.sequence,
                            transition_id: rec.id.clone(),
                            entity_id: rec.entity_id.clone(),
                            machine: rec.machine.clone(),
                            from_state: rec.from_state.clone(),
                            to_state: rec.to_state.clone(),
                            event: rec.event.clone(),
                            actor: rec.actor.clone(),
                            version: rec.version,
                            timestamp: rec.timestamp,
                            ctx: rec.ctx.clone(),
                            effects: effects
                                .iter()
                                .map(|e| EffectPayload {
                                    effect_name: e.effect_name.clone(),
                                    payload: e.payload.clone(),
                                })
                                .collect(),
                        };
                        if sub.sender.send(change).is_err() {
                            dead.push(sub.id.clone());
                            sub.delivered_through.store(cursor, Ordering::Release);
                            break 'drain;
                        }
                        delivered += 1;
                    }
                    sub.delivered_through.store(cursor, Ordering::Release);
                    if batch_len < BATCH as usize {
                        break;
                    }
                }
            }
        }

        if !dead.is_empty() {
            self.subscribers.write().retain(|s| !dead.contains(&s.id));
        }
        delivered
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use smdb_core::prelude::MachineBuilder;
    use smdb_storage::RedbEngine;

    fn test_engine() -> Engine {
        let storage = Arc::new(RedbEngine::in_memory().unwrap());
        Engine::new(storage)
    }

    fn order_machine() -> MachineDefinition {
        MachineBuilder::new()
            .name("fulfillment")
            .version(1)
            .states([
                "pending",
                "paid",
                "packed",
                "shipped",
                "delivered",
                "canceled",
            ])
            .initial_state("pending")
            .transition("pay", ["pending"], "paid")
            .transition("pack", ["paid"], "packed")
            .transition_with_guards("ship", ["packed"], "shipped", ["payment_captured"])
            .transition("deliver", ["shipped"], "delivered")
            .transition("cancel", ["pending", "paid", "packed"], "canceled")
            .effect("ship", "notify_customer", None)
            .build()
            .unwrap()
    }

    #[test]
    fn define_and_retrieve_machine() {
        let engine = test_engine();
        let machine = order_machine();
        engine.define_machine(machine.clone()).unwrap();
        let retrieved = engine.get_machine("fulfillment").unwrap();
        assert_eq!(retrieved.name, "fulfillment");
        assert_eq!(retrieved.states.len(), 6);
    }

    #[test]
    fn basic_transition() {
        let engine = test_engine();
        engine.define_machine(order_machine()).unwrap();

        let result = engine
            .transition(
                "order_1",
                "fulfillment",
                "pay",
                "user:alice",
                serde_json::json!({}),
                None,
                None,
            )
            .unwrap();

        assert_eq!(result.from_state, "pending");
        assert_eq!(result.to_state, "paid");
        assert_eq!(result.version, 1);

        let state = engine.current("order_1", "fulfillment").unwrap();
        assert_eq!(state.current_state, "paid");
        assert_eq!(state.version, 1);
    }

    #[test]
    fn illegal_transition_rejected() {
        let engine = test_engine();
        engine.define_machine(order_machine()).unwrap();

        let result = engine.transition(
            "order_1",
            "fulfillment",
            "deliver",
            "user:alice",
            serde_json::json!({}),
            None,
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn version_conflict() {
        let engine = test_engine();
        engine.define_machine(order_machine()).unwrap();
        engine
            .transition(
                "order_1",
                "fulfillment",
                "pay",
                "user:alice",
                serde_json::json!({}),
                None,
                None,
            )
            .unwrap();

        let result = engine.transition(
            "order_1",
            "fulfillment",
            "pack",
            "user:bob",
            serde_json::json!({}),
            Some(0),
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn guard_blocks_transition() {
        let engine = test_engine();
        engine.define_machine(order_machine()).unwrap();
        engine.register_guard("payment_captured", Arc::new(|_, _| false));

        engine
            .transition(
                "order_1",
                "fulfillment",
                "pay",
                "u",
                serde_json::json!({}),
                None,
                None,
            )
            .unwrap();
        engine
            .transition(
                "order_1",
                "fulfillment",
                "pack",
                "u",
                serde_json::json!({}),
                None,
                None,
            )
            .unwrap();

        let result = engine.transition(
            "order_1",
            "fulfillment",
            "ship",
            "u",
            serde_json::json!({}),
            None,
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn history_tracks_transitions() {
        let engine = test_engine();
        engine.define_machine(order_machine()).unwrap();
        engine
            .transition(
                "order_1",
                "fulfillment",
                "pay",
                "u",
                serde_json::json!({}),
                None,
                None,
            )
            .unwrap();
        engine
            .transition(
                "order_1",
                "fulfillment",
                "pack",
                "u",
                serde_json::json!({}),
                None,
                None,
            )
            .unwrap();

        let history = engine
            .history("order_1", "fulfillment", None, None)
            .unwrap();
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].from_state, "pending");
        assert_eq!(history[1].from_state, "paid");
    }

    #[test]
    fn created_at_is_preserved_across_transitions() {
        let engine = test_engine();
        engine.define_machine(order_machine()).unwrap();

        engine
            .transition(
                "order_1",
                "fulfillment",
                "pay",
                "u",
                serde_json::json!({}),
                None,
                None,
            )
            .unwrap();
        let after_first = engine.current("order_1", "fulfillment").unwrap();

        engine
            .transition(
                "order_1",
                "fulfillment",
                "pack",
                "u",
                serde_json::json!({}),
                None,
                None,
            )
            .unwrap();
        let after_second = engine.current("order_1", "fulfillment").unwrap();

        // created_at must carry forward unchanged; updated_at advances.
        assert_eq!(after_first.created_at, after_second.created_at);
        assert!(after_second.updated_at >= after_first.updated_at);
        assert_eq!(after_second.version, 2);
    }

    #[test]
    fn idempotent_replay_returns_real_version() {
        let engine = test_engine();
        engine.define_machine(order_machine()).unwrap();

        let first = engine
            .transition(
                "order_1",
                "fulfillment",
                "pay",
                "u",
                serde_json::json!({}),
                None,
                Some("idem-1".to_string()),
            )
            .unwrap();
        let replay = engine
            .transition(
                "order_1",
                "fulfillment",
                "pay",
                "u",
                serde_json::json!({}),
                None,
                Some("idem-1".to_string()),
            )
            .unwrap();

        assert_eq!(replay.sequence, first.sequence);
        assert_eq!(replay.transition_id, first.transition_id);
        assert_eq!(replay.version, 1);
    }

    #[test]
    fn dispatch_delivers_ordered_change_records_with_effects() {
        let engine = test_engine();
        engine.define_machine(order_machine()).unwrap();
        engine.register_guard("payment_captured", Arc::new(|_, _| true));

        let mut rx = engine
            .subscribe("sub1".to_string(), Some("fulfillment".to_string()), 0)
            .unwrap();

        engine
            .transition(
                "o1",
                "fulfillment",
                "pay",
                "u",
                serde_json::json!({}),
                None,
                None,
            )
            .unwrap();
        engine
            .transition(
                "o1",
                "fulfillment",
                "pack",
                "u",
                serde_json::json!({}),
                None,
                None,
            )
            .unwrap();
        engine
            .transition(
                "o1",
                "fulfillment",
                "ship",
                "u",
                serde_json::json!({}),
                None,
                None,
            )
            .unwrap();

        assert_eq!(engine.dispatch_pass(), 3);
        // A second pass with no new commits delivers nothing (cursor persists).
        assert_eq!(engine.dispatch_pass(), 0);

        let mut recs = Vec::new();
        while let Ok(r) = rx.try_recv() {
            recs.push(r);
        }
        assert_eq!(recs.len(), 3);
        assert_eq!(
            recs.iter().map(|r| r.sequence).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        // The "ship" record carries its reconstructed effect and post-version.
        assert_eq!(recs[2].event, "ship");
        assert_eq!(recs[2].version, 3);
        assert_eq!(recs[2].effects.len(), 1);
        assert_eq!(recs[2].effects[0].effect_name, "notify_customer");
    }

    #[test]
    fn dispatch_respects_machine_filter() {
        let engine = test_engine();
        engine.define_machine(order_machine()).unwrap();
        let payment = MachineBuilder::new()
            .name("payment")
            .version(1)
            .states(["unpaid", "captured"])
            .initial_state("unpaid")
            .transition("capture", ["unpaid"], "captured")
            .build()
            .unwrap();
        engine.define_machine(payment).unwrap();

        let mut rx = engine
            .subscribe("sub1".to_string(), Some("payment".to_string()), 0)
            .unwrap();

        engine
            .transition(
                "o1",
                "fulfillment",
                "pay",
                "u",
                serde_json::json!({}),
                None,
                None,
            )
            .unwrap();
        engine
            .transition(
                "o1",
                "payment",
                "capture",
                "u",
                serde_json::json!({}),
                None,
                None,
            )
            .unwrap();

        engine.dispatch_pass();
        let mut recs = Vec::new();
        while let Ok(r) = rx.try_recv() {
            recs.push(r);
        }
        // Only the payment-machine record is delivered; the fulfillment one is
        // skipped but its sequence is still consumed (no infinite re-scan).
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].machine, "payment");
    }

    #[test]
    fn multiple_machines_on_one_entity() {
        let engine = test_engine();
        engine.define_machine(order_machine()).unwrap();

        let payment = MachineBuilder::new()
            .name("payment")
            .version(1)
            .states(["unpaid", "captured", "refunded"])
            .initial_state("unpaid")
            .transition("capture", ["unpaid"], "captured")
            .transition("refund", ["captured"], "refunded")
            .build()
            .unwrap();
        engine.define_machine(payment).unwrap();

        engine
            .transition(
                "order_1",
                "fulfillment",
                "pay",
                "u",
                serde_json::json!({}),
                None,
                None,
            )
            .unwrap();
        engine
            .transition(
                "order_1",
                "payment",
                "capture",
                "u",
                serde_json::json!({}),
                None,
                None,
            )
            .unwrap();

        let f = engine.current("order_1", "fulfillment").unwrap();
        let p = engine.current("order_1", "payment").unwrap();
        assert_eq!(f.current_state, "paid");
        assert_eq!(p.current_state, "captured");
    }
}
