use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, watch};

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
}

pub struct Engine {
    storage: Arc<dyn StorageEngine>,
    guards: Arc<RwLock<GuardRegistry>>,
    entity_locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    subscribers: Arc<RwLock<Vec<Subscriber>>>,
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
}

impl Engine {
    pub fn new(storage: Arc<dyn StorageEngine>) -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        Self {
            storage,
            guards: Arc::new(RwLock::new(GuardRegistry::new())),
            entity_locks: Mutex::new(HashMap::new()),
            subscribers: Arc::new(RwLock::new(Vec::new())),
            shutdown_tx,
            shutdown_rx,
        }
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

        if let Some(ref key) = idempotency_key {
            if let Ok(Some(existing)) = self.storage.check_idempotency(key) {
                return Ok(TransitionResult {
                    entity_id: existing.entity_id.clone(),
                    machine: existing.machine.clone(),
                    from_state: existing.from_state.clone(),
                    to_state: existing.to_state.clone(),
                    version: 0,
                    transition_id: existing.id.clone(),
                    sequence: existing.sequence,
                    timestamp: existing.timestamp,
                });
            }
        }

        let lock_key = format!("{}:{}", entity_id, machine_name);
        let entity_lock = {
            let mut locks = self.entity_locks.lock();
            locks
                .entry(lock_key)
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                .clone()
        };
        let _guard = entity_lock.blocking_lock();

        let definition = self.storage.get_machine(machine_name, None)?;

        let (current_state, current_version) = match self.storage.get_entity_state(entity_id, machine_name) {
            Ok(state) => (state.current_state.clone(), state.version),
            Err(smdb_storage::StorageError::NotFound(_)) => {
                (definition.initial_state.clone(), 0)
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

        let rule = FsmPlanner::plan_transition(&definition, &current_state, &event.to_string(), &entity_id.to_string())?;

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
            .map(|er| Effect::new(record.id.clone(), er.effect.clone(), er.payload.clone().unwrap_or_default()))
            .collect();

        let new_version = current_version + 1;
        let now = Utc::now();
        let new_state = EntityState {
            entity_id: entity_id.to_string(),
            machine: machine_name.to_string(),
            current_state: rule.to_state.clone(),
            version: new_version,
            updated_at: now,
            created_at: if current_version == 0 { now } else { now },
        };

        let sequence = self.storage.execute_transition(&mut record, &new_state, &effects)?;

        let change = ChangeRecord {
            sequence,
            transition_id: record.id.clone(),
            entity_id: entity_id.to_string(),
            machine: machine_name.to_string(),
            from_state: current_state.clone(),
            to_state: rule.to_state.clone(),
            event: event.to_string(),
            actor: actor.to_string(),
            version: new_version,
            timestamp: record.timestamp,
            ctx,
            effects: effects
                .iter()
                .map(|e| EffectPayload {
                    effect_name: e.effect_name.clone(),
                    payload: e.payload.clone(),
                })
                .collect(),
        };

        {
            let subs = self.subscribers.read();
            for sub in subs.iter() {
                if let Some(ref filter) = sub.machine_filter {
                    if filter != machine_name {
                        continue;
                    }
                }
                let _ = sub.sender.send(change.clone());
            }
        }

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

    pub fn subscribe(
        &self,
        id: String,
        machine_filter: Option<String>,
        after_sequence: Sequence,
    ) -> Result<mpsc::UnboundedReceiver<ChangeRecord>> {
        let (tx, rx) = mpsc::unbounded_channel();

        if let Ok(records) = self.storage.get_transitions_after(after_sequence, 10_000) {
            for rec in records {
                if let Some(ref filter) = machine_filter {
                    if &rec.machine != filter {
                        continue;
                    }
                }
                let change = ChangeRecord {
                    sequence: rec.sequence,
                    transition_id: rec.id.clone(),
                    entity_id: rec.entity_id.clone(),
                    machine: rec.machine.clone(),
                    from_state: rec.from_state.clone(),
                    to_state: rec.to_state.clone(),
                    event: rec.event.clone(),
                    actor: rec.actor.clone(),
                    version: 0,
                    timestamp: rec.timestamp,
                    ctx: rec.ctx.clone(),
                    effects: vec![],
                };
                let _ = tx.send(change);
            }
        }

        self.subscribers.write().push(Subscriber {
            id,
            machine_filter,
            sender: tx,
        });

        Ok(rx)
    }

    pub fn unsubscribe(&self, id: &str) {
        self.subscribers.write().retain(|s| s.id != id);
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
            .states(["pending", "paid", "packed", "shipped", "delivered", "canceled"])
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
            .transition("order_1", "fulfillment", "pay", "user:alice", serde_json::json!({}), None, None)
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
            "order_1", "fulfillment", "deliver", "user:alice", serde_json::json!({}), None, None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn version_conflict() {
        let engine = test_engine();
        engine.define_machine(order_machine()).unwrap();
        engine
            .transition("order_1", "fulfillment", "pay", "user:alice", serde_json::json!({}), None, None)
            .unwrap();

        let result = engine.transition(
            "order_1", "fulfillment", "pack", "user:bob", serde_json::json!({}), Some(0), None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn guard_blocks_transition() {
        let engine = test_engine();
        engine.define_machine(order_machine()).unwrap();
        engine.register_guard("payment_captured", Arc::new(|_, _| false));

        engine
            .transition("order_1", "fulfillment", "pay", "u", serde_json::json!({}), None, None)
            .unwrap();
        engine
            .transition("order_1", "fulfillment", "pack", "u", serde_json::json!({}), None, None)
            .unwrap();

        let result = engine.transition(
            "order_1", "fulfillment", "ship", "u", serde_json::json!({}), None, None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn history_tracks_transitions() {
        let engine = test_engine();
        engine.define_machine(order_machine()).unwrap();
        engine
            .transition("order_1", "fulfillment", "pay", "u", serde_json::json!({}), None, None)
            .unwrap();
        engine
            .transition("order_1", "fulfillment", "pack", "u", serde_json::json!({}), None, None)
            .unwrap();

        let history = engine.history("order_1", "fulfillment", None, None).unwrap();
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].from_state, "pending");
        assert_eq!(history[1].from_state, "paid");
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
            .transition("order_1", "fulfillment", "pay", "u", serde_json::json!({}), None, None)
            .unwrap();
        engine
            .transition("order_1", "payment", "capture", "u", serde_json::json!({}), None, None)
            .unwrap();

        let f = engine.current("order_1", "fulfillment").unwrap();
        let p = engine.current("order_1", "payment").unwrap();
        assert_eq!(f.current_state, "paid");
        assert_eq!(p.current_state, "captured");
    }
}
