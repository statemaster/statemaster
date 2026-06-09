use std::collections::HashMap;
use std::sync::Arc;

use smdb_core::prelude::EntityState;

/// A guard function: given the current entity state and an arbitrary JSON
/// context, returns `true` if the guard passes and the transition may proceed.
pub type GuardFn = Arc<dyn Fn(&EntityState, &serde_json::Value) -> bool + Send + Sync>;

/// A named registry of guard predicates. The engine holds this behind an
/// `RwLock` so guards can be registered at any time without stopping reads.
pub struct GuardRegistry {
    guards: HashMap<String, GuardFn>,
}

impl GuardRegistry {
    pub fn new() -> Self {
        Self {
            guards: HashMap::new(),
        }
    }

    /// Register (or overwrite) a named guard.
    pub fn register(&mut self, name: &str, guard: GuardFn) {
        self.guards.insert(name.to_string(), guard);
    }

    /// Evaluate a named guard against the given entity state and context.
    ///
    /// Returns `true` if the guard is registered and passes, or if the guard
    /// is **not registered** (default-allow for unknown guards). Returns
    /// `false` only when the guard is explicitly registered and returns
    /// `false`.
    pub fn evaluate(&self, name: &str, state: &EntityState, ctx: &serde_json::Value) -> bool {
        match self.guards.get(name) {
            Some(f) => f(state, ctx),
            None => true, // default-allow for unregistered guards
        }
    }
}

impl Default for GuardRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;

    fn make_state() -> EntityState {
        EntityState {
            entity_id: "e1".into(),
            machine: "m1".into(),
            current_state: "pending".into(),
            version: 0,
            updated_at: Utc::now(),
            created_at: Utc::now(),
        }
    }

    #[test]
    fn missing_guard_defaults_to_true() {
        let registry = GuardRegistry::new();
        let state = make_state();
        assert!(registry.evaluate("nonexistent", &state, &serde_json::json!({})));
    }

    #[test]
    fn registered_guard_returning_true_passes() {
        let mut registry = GuardRegistry::new();
        registry.register("always_pass", Arc::new(|_, _| true));
        let state = make_state();
        assert!(registry.evaluate("always_pass", &state, &serde_json::json!({})));
    }

    #[test]
    fn registered_guard_returning_false_fails() {
        let mut registry = GuardRegistry::new();
        registry.register("always_fail", Arc::new(|_, _| false));
        let state = make_state();
        assert!(!registry.evaluate("always_fail", &state, &serde_json::json!({})));
    }

    #[test]
    fn guard_can_inspect_context() {
        let mut registry = GuardRegistry::new();
        registry.register(
            "check_amount",
            Arc::new(|_, ctx| ctx["amount"].as_u64().unwrap_or(0) > 0),
        );
        let state = make_state();
        assert!(registry.evaluate("check_amount", &state, &serde_json::json!({ "amount": 50 })));
        assert!(!registry.evaluate("check_amount", &state, &serde_json::json!({ "amount": 0 })));
    }
}
