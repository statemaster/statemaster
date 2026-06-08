use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::{CoreError, Result};
use crate::types::{EffectName, EventName, GuardName, MachineName, StateName};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransitionRule {
    pub event: EventName,
    pub from_states: Vec<StateName>,
    pub to_state: StateName,
    pub guards: Vec<GuardName>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EffectRule {
    pub on_event: EventName,
    pub effect: EffectName,
    pub payload: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MachineDefinition {
    pub name: MachineName,
    pub version: u32,
    pub states: Vec<StateName>,
    pub initial_state: StateName,
    pub transitions: Vec<TransitionRule>,
    pub effects: Vec<EffectRule>,
    pub created_at: DateTime<Utc>,
}

impl MachineDefinition {
    pub fn validate(&self) -> Result<()> {
        // Check for duplicate states.
        let mut seen_states = std::collections::HashSet::new();
        for state in &self.states {
            if !seen_states.insert(state.as_str()) {
                return Err(CoreError::DuplicateState { name: state.clone() });
            }
        }

        // Initial state must exist.
        if !seen_states.contains(self.initial_state.as_str()) {
            return Err(CoreError::InvalidDefinition {
                reason: format!(
                    "initial_state '{}' is not in the states list",
                    self.initial_state
                ),
            });
        }

        // Validate all states referenced in transitions exist, and check for
        // duplicate (event, from_state) pairs which would make the machine
        // non-deterministic.
        let mut pair_seen = std::collections::HashSet::new();
        for rule in &self.transitions {
            for from in &rule.from_states {
                if !seen_states.contains(from.as_str()) {
                    return Err(CoreError::InvalidDefinition {
                        reason: format!(
                            "transition '{}' references unknown from_state '{}'",
                            rule.event, from
                        ),
                    });
                }
                let pair = (rule.event.as_str(), from.as_str());
                if !pair_seen.insert(pair) {
                    return Err(CoreError::InvalidDefinition {
                        reason: format!(
                            "duplicate (event, from_state) pair: ('{}', '{}')",
                            rule.event, from
                        ),
                    });
                }
            }
            if !seen_states.contains(rule.to_state.as_str()) {
                return Err(CoreError::InvalidDefinition {
                    reason: format!(
                        "transition '{}' references unknown to_state '{}'",
                        rule.event, rule.to_state
                    ),
                });
            }
        }

        Ok(())
    }
}

// Builder for constructing MachineDefinitions with a fluent API.
#[derive(Debug, Default)]
pub struct MachineBuilder {
    name: Option<MachineName>,
    version: u32,
    states: Vec<StateName>,
    initial_state: Option<StateName>,
    transitions: Vec<TransitionRule>,
    effects: Vec<EffectRule>,
}

impl MachineBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn name(mut self, name: impl Into<MachineName>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub fn version(mut self, version: u32) -> Self {
        self.version = version;
        self
    }

    pub fn state(mut self, state: impl Into<StateName>) -> Self {
        self.states.push(state.into());
        self
    }

    pub fn states(mut self, states: impl IntoIterator<Item = impl Into<StateName>>) -> Self {
        self.states.extend(states.into_iter().map(|s| s.into()));
        self
    }

    pub fn initial_state(mut self, state: impl Into<StateName>) -> Self {
        self.initial_state = Some(state.into());
        self
    }

    pub fn transition(
        mut self,
        event: impl Into<EventName>,
        from_states: impl IntoIterator<Item = impl Into<StateName>>,
        to_state: impl Into<StateName>,
    ) -> Self {
        self.transitions.push(TransitionRule {
            event: event.into(),
            from_states: from_states.into_iter().map(|s| s.into()).collect(),
            to_state: to_state.into(),
            guards: vec![],
        });
        self
    }

    pub fn transition_with_guards(
        mut self,
        event: impl Into<EventName>,
        from_states: impl IntoIterator<Item = impl Into<StateName>>,
        to_state: impl Into<StateName>,
        guards: impl IntoIterator<Item = impl Into<GuardName>>,
    ) -> Self {
        self.transitions.push(TransitionRule {
            event: event.into(),
            from_states: from_states.into_iter().map(|s| s.into()).collect(),
            to_state: to_state.into(),
            guards: guards.into_iter().map(|g| g.into()).collect(),
        });
        self
    }

    pub fn effect(
        mut self,
        on_event: impl Into<EventName>,
        effect: impl Into<EffectName>,
        payload: Option<serde_json::Value>,
    ) -> Self {
        self.effects.push(EffectRule {
            on_event: on_event.into(),
            effect: effect.into(),
            payload,
        });
        self
    }

    pub fn build(self) -> Result<MachineDefinition> {
        let name = self.name.ok_or_else(|| CoreError::InvalidDefinition {
            reason: "machine name is required".into(),
        })?;
        let initial_state = self.initial_state.ok_or_else(|| CoreError::InvalidDefinition {
            reason: "initial_state is required".into(),
        })?;

        let def = MachineDefinition {
            name,
            version: self.version,
            states: self.states,
            initial_state,
            transitions: self.transitions,
            effects: self.effects,
            created_at: Utc::now(),
        };

        def.validate()?;
        Ok(def)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn valid_machine_builds_and_validates() {
        let def = fulfillment_machine();
        assert_eq!(def.name, "fulfillment");
        assert_eq!(def.states.len(), 6);
        assert_eq!(def.transitions.len(), 5);
    }

    #[test]
    fn duplicate_state_is_rejected() {
        let result = MachineBuilder::new()
            .name("m")
            .states(["a", "a"])
            .initial_state("a")
            .build();
        assert!(matches!(result, Err(CoreError::DuplicateState { .. })));
    }

    #[test]
    fn unknown_initial_state_is_rejected() {
        let result = MachineBuilder::new()
            .name("m")
            .state("a")
            .initial_state("b")
            .build();
        assert!(matches!(result, Err(CoreError::InvalidDefinition { .. })));
    }

    #[test]
    fn unknown_from_state_in_transition_is_rejected() {
        let result = MachineBuilder::new()
            .name("m")
            .state("a")
            .initial_state("a")
            .transition("go", ["b"], "a")
            .build();
        assert!(matches!(result, Err(CoreError::InvalidDefinition { .. })));
    }

    #[test]
    fn unknown_to_state_in_transition_is_rejected() {
        let result = MachineBuilder::new()
            .name("m")
            .state("a")
            .initial_state("a")
            .transition("go", ["a"], "z")
            .build();
        assert!(matches!(result, Err(CoreError::InvalidDefinition { .. })));
    }

    #[test]
    fn duplicate_event_from_state_pair_is_rejected() {
        let result = MachineBuilder::new()
            .name("m")
            .states(["a", "b"])
            .initial_state("a")
            .transition("go", ["a"], "b")
            .transition("go", ["a"], "b")
            .build();
        assert!(matches!(result, Err(CoreError::InvalidDefinition { .. })));
    }

    #[test]
    fn cancel_from_multiple_states_has_three_pairs() {
        let def = fulfillment_machine();
        let cancel = def.transitions.iter().find(|t| t.event == "cancel").unwrap();
        assert_eq!(cancel.from_states.len(), 3);
    }
}
