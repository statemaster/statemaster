use std::collections::HashMap;

use chrono::Utc;

use crate::error::{CoreError, Result};
use crate::machine::{EffectRule, MachineDefinition, TransitionRule};
use crate::transition::TransitionRecord;
use crate::types::{
    ActorId, Context, EntityId, EventName, GuardName, IdempotencyKey, MachineName, StateName,
};

pub struct FsmPlanner;

impl FsmPlanner {
    /// Looks up the transition rule for `(current_state, event)`.
    ///
    /// Returns `IllegalTransition` if no matching rule exists. Because
    /// `MachineDefinition::validate()` ensures no duplicate (event, from_state)
    /// pairs, this lookup is always deterministic.
    pub fn plan_transition<'a>(
        definition: &'a MachineDefinition,
        current_state: &StateName,
        event: &EventName,
        entity_id: &EntityId,
    ) -> Result<&'a TransitionRule> {
        definition
            .transitions
            .iter()
            .find(|rule| {
                rule.event == *event && rule.from_states.iter().any(|s| s == current_state)
            })
            .ok_or_else(|| CoreError::IllegalTransition {
                entity_id: entity_id.clone(),
                machine: definition.name.clone(),
                event: event.clone(),
                current_state: current_state.clone(),
            })
    }

    /// Checks that every guard declared on `rule` has a `true` result in
    /// `guard_results`. Returns `GuardFailed` for the first guard that either
    /// evaluated to `false` or was absent from the results map.
    pub fn validate_guards(
        rule: &TransitionRule,
        guard_results: &HashMap<GuardName, bool>,
    ) -> Result<()> {
        for guard in &rule.guards {
            let passed = guard_results.get(guard).copied().unwrap_or(false);
            if !passed {
                return Err(CoreError::GuardFailed {
                    guard_name: guard.clone(),
                    reason: "guard evaluated to false".into(),
                });
            }
        }
        Ok(())
    }

    /// Returns all effect rules that fire on `event`. There may be zero or
    /// more effects per event; the caller is responsible for materialising them
    /// into `Effect` rows via the outbox.
    pub fn compute_effects<'a>(
        definition: &'a MachineDefinition,
        event: &EventName,
    ) -> Vec<&'a EffectRule> {
        definition
            .effects
            .iter()
            .filter(|e| e.on_event == *event)
            .collect()
    }

    /// Constructs the immutable `TransitionRecord`. `sequence` is set to 0
    /// (unassigned) — storage assigns the final value when persisting.
    pub fn build_transition_record(
        entity_id: EntityId,
        machine: MachineName,
        from_state: StateName,
        to_state: StateName,
        event: EventName,
        actor: ActorId,
        ctx: Context,
        idempotency_key: Option<IdempotencyKey>,
    ) -> TransitionRecord {
        TransitionRecord {
            id: uuid::Uuid::new_v4().to_string(),
            sequence: 0,
            entity_id,
            machine,
            from_state,
            to_state,
            event,
            actor,
            ctx,
            idempotency_key,
            version: 0,
            timestamp: Utc::now(),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::machine::MachineBuilder;

    fn fulfillment_machine() -> MachineDefinition {
        MachineBuilder::new()
            .name("fulfillment")
            .version(1)
            .states(["pending", "paid", "packed", "shipped", "delivered", "canceled"])
            .initial_state("pending")
            .transition("pay", ["pending"], "paid")
            .transition_with_guards("pack", ["paid"], "packed", ["inventory_reserved"])
            .transition_with_guards("ship", ["packed"], "shipped", ["payment_captured"])
            .transition("deliver", ["shipped"], "delivered")
            .transition("cancel", ["pending", "paid", "packed"], "canceled")
            .effect("ship", "notify_customer", None)
            .effect("cancel", "release_inventory", None)
            .build()
            .unwrap()
    }

    #[test]
    fn plan_transition_finds_correct_rule() {
        let def = fulfillment_machine();
        let entity_id = "order_1".to_string();
        let rule = FsmPlanner::plan_transition(
            &def,
            &"pending".to_string(),
            &"pay".to_string(),
            &entity_id,
        )
        .unwrap();
        assert_eq!(rule.to_state, "paid");
    }

    #[test]
    fn plan_transition_returns_illegal_transition_for_wrong_state() {
        let def = fulfillment_machine();
        let entity_id = "order_1".to_string();
        let err = FsmPlanner::plan_transition(
            &def,
            &"delivered".to_string(),
            &"ship".to_string(),
            &entity_id,
        )
        .unwrap_err();
        assert!(matches!(err, CoreError::IllegalTransition { .. }));
    }

    #[test]
    fn plan_transition_returns_illegal_transition_for_unknown_event() {
        let def = fulfillment_machine();
        let entity_id = "order_1".to_string();
        let err = FsmPlanner::plan_transition(
            &def,
            &"pending".to_string(),
            &"teleport".to_string(),
            &entity_id,
        )
        .unwrap_err();
        assert!(matches!(err, CoreError::IllegalTransition { .. }));
    }

    #[test]
    fn cancel_is_legal_from_multiple_states() {
        let def = fulfillment_machine();
        let entity_id = "order_1".to_string();
        for from in ["pending", "paid", "packed"] {
            let rule = FsmPlanner::plan_transition(
                &def,
                &from.to_string(),
                &"cancel".to_string(),
                &entity_id,
            )
            .unwrap();
            assert_eq!(rule.to_state, "canceled");
        }
    }

    #[test]
    fn validate_guards_passes_when_all_guards_true() {
        let def = fulfillment_machine();
        let entity_id = "order_1".to_string();
        let rule = FsmPlanner::plan_transition(
            &def,
            &"packed".to_string(),
            &"ship".to_string(),
            &entity_id,
        )
        .unwrap();
        let mut results = HashMap::new();
        results.insert("payment_captured".to_string(), true);
        assert!(FsmPlanner::validate_guards(rule, &results).is_ok());
    }

    #[test]
    fn validate_guards_fails_on_false_guard() {
        let def = fulfillment_machine();
        let entity_id = "order_1".to_string();
        let rule = FsmPlanner::plan_transition(
            &def,
            &"packed".to_string(),
            &"ship".to_string(),
            &entity_id,
        )
        .unwrap();
        let mut results = HashMap::new();
        results.insert("payment_captured".to_string(), false);
        let err = FsmPlanner::validate_guards(rule, &results).unwrap_err();
        assert!(matches!(
            err,
            CoreError::GuardFailed { guard_name, .. } if guard_name == "payment_captured"
        ));
    }

    #[test]
    fn validate_guards_fails_when_guard_absent_from_results() {
        let def = fulfillment_machine();
        let entity_id = "order_1".to_string();
        let rule = FsmPlanner::plan_transition(
            &def,
            &"packed".to_string(),
            &"ship".to_string(),
            &entity_id,
        )
        .unwrap();
        // No guard results provided — absent guards default to false.
        let err = FsmPlanner::validate_guards(rule, &HashMap::new()).unwrap_err();
        assert!(matches!(err, CoreError::GuardFailed { .. }));
    }

    #[test]
    fn validate_guards_passes_when_no_guards_on_rule() {
        let def = fulfillment_machine();
        let entity_id = "order_1".to_string();
        // "pay" has no guards.
        let rule = FsmPlanner::plan_transition(
            &def,
            &"pending".to_string(),
            &"pay".to_string(),
            &entity_id,
        )
        .unwrap();
        assert!(FsmPlanner::validate_guards(rule, &HashMap::new()).is_ok());
    }

    #[test]
    fn compute_effects_returns_effects_for_event() {
        let def = fulfillment_machine();
        let effects = FsmPlanner::compute_effects(&def, &"ship".to_string());
        assert_eq!(effects.len(), 1);
        assert_eq!(effects[0].effect, "notify_customer");
    }

    #[test]
    fn compute_effects_returns_empty_for_event_with_no_effects() {
        let def = fulfillment_machine();
        let effects = FsmPlanner::compute_effects(&def, &"pay".to_string());
        assert!(effects.is_empty());
    }

    #[test]
    fn build_transition_record_produces_valid_record() {
        let record = FsmPlanner::build_transition_record(
            "order_1".into(),
            "fulfillment".into(),
            "pending".into(),
            "paid".into(),
            "pay".into(),
            "svc:billing".into(),
            json!({ "amount": 100 }),
            Some("idem-key-1".into()),
        );
        assert_eq!(record.entity_id, "order_1");
        assert_eq!(record.from_state, "pending");
        assert_eq!(record.to_state, "paid");
        assert_eq!(record.sequence, 0);
        assert!(record.idempotency_key.is_some());
        // UUID v4 is 36 characters.
        assert_eq!(record.id.len(), 36);
    }
}
