use crate::transform::{error::TransformError, pipeline::Validator};
use engine_core::context::env::EnvContext;
use engine_wasm::{
    exchange::types::{FilterDecision, PluginInput},
    registry::PluginRegistry,
    runtime::instance::PluginInstance,
};
use expression_engine::eval::runtime::Evaluator;
use model::{
    core::value::Value,
    execution::{
        expr::CompiledExpression,
        pipeline::{ValidationKind, ValidationRule},
    },
    records::Record,
    transform::mapping::TransformationMetadata,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationAction {
    Skip, // Filter out the row
    Fail, // Stop the pipeline
    Warn, // Log warning but continue
}

pub enum ValidationResult {
    Pass,
    Failed {
        rule: String,
        message: String,
        action: ValidationAction,
    },
}

/// Pre-built per-rule state. Indexed by rule position.
enum CompiledRule {
    Assert,
    WasmFilter {
        plugin: Box<Mutex<PluginInstance>>,
        plugin_name: String,
    },
}

/// Validator that evaluates validation rules from a Pipeline. Supports both
/// expression-based asserts and WASM filter plugins.
pub struct PipelineValidator {
    rules: Vec<ValidationRule>,
    compiled: Vec<CompiledRule>,
    metadata: TransformationMetadata,
    env: Arc<EnvContext>,
}

impl PipelineValidator {
    pub fn new(
        rules: Vec<ValidationRule>,
        metadata: TransformationMetadata,
        env: Arc<EnvContext>,
        plugin_registry: &PluginRegistry,
    ) -> Result<Self, TransformError> {
        let compiled = rules
            .iter()
            .map(|rule| match &rule.kind {
                ValidationKind::Assert { .. } => Ok(CompiledRule::Assert),
                ValidationKind::WasmFilter { plugin_name, .. } => {
                    let plugin = plugin_registry.instantiate(plugin_name).map_err(|e| {
                        TransformError::Transformation(format!(
                            "validation plugin '{plugin_name}' instantiation failed: {e}"
                        ))
                    })?;
                    Ok(CompiledRule::WasmFilter {
                        plugin: Box::new(Mutex::new(plugin)),
                        plugin_name: plugin_name.clone(),
                    })
                }
            })
            .collect::<Result<Vec<_>, TransformError>>()?;

        Ok(Self {
            rules,
            compiled,
            metadata,
            env,
        })
    }

    fn evaluate_assert(
        &self,
        check: &CompiledExpression,
        rule_label: &str,
        row: &Record,
    ) -> Result<bool, TransformError> {
        let env = self.env.clone();
        let env_getter = move |key: &str| env.get(key);
        match check.evaluate(row, &self.metadata, &env_getter) {
            Some(Value::Boolean(true)) => Ok(true),
            Some(Value::Boolean(false)) => Ok(false),
            Some(_) => Err(TransformError::Transformation(format!(
                "Validation rule '{rule_label}' returned non-boolean value"
            ))),
            None => Ok(false),
        }
    }

    fn evaluate_wasm(
        plugin: &Mutex<PluginInstance>,
        plugin_name: &str,
        input_mapping: &HashMap<String, String>,
        row: &Record,
    ) -> Result<(bool, Option<String>), TransformError> {
        let input = PluginInput::from_record(row, input_mapping);
        let mut guard = plugin.lock().expect("validation plugin mutex poisoned");
        let decision = guard.call_evaluate(&input).map_err(|e| {
            TransformError::Transformation(format!("wasm filter '{plugin_name}' failed: {e}"))
        })?;
        Ok(match decision {
            FilterDecision::Pass => (true, None),
            FilterDecision::Reject { reason } => (false, Some(reason)),
        })
    }
}

impl Validator for PipelineValidator {
    fn validate(&self, row: &Record) -> Result<ValidationResult, TransformError> {
        for (rule, compiled) in self.rules.iter().zip(self.compiled.iter()) {
            let (passed, reject_reason) = match (&rule.kind, compiled) {
                (ValidationKind::Assert { check }, CompiledRule::Assert) => {
                    (self.evaluate_assert(check, &rule.label, row)?, None)
                }
                (
                    ValidationKind::WasmFilter { input_mapping, .. },
                    CompiledRule::WasmFilter {
                        plugin,
                        plugin_name,
                    },
                ) => Self::evaluate_wasm(plugin, plugin_name, input_mapping, row)?,
                _ => unreachable!("rules and compiled state diverged"),
            };

            if !passed {
                let action = match rule.action {
                    model::execution::pipeline::ValidationAction::Skip => ValidationAction::Skip,
                    model::execution::pipeline::ValidationAction::Fail => ValidationAction::Fail,
                    model::execution::pipeline::ValidationAction::Warn => ValidationAction::Warn,
                    model::execution::pipeline::ValidationAction::Continue => {
                        ValidationAction::Warn
                    }
                };

                let message = reject_reason.unwrap_or_else(|| rule.message.clone());
                return Ok(ValidationResult::Failed {
                    rule: rule.label.clone(),
                    message,
                    action,
                });
            }
        }

        Ok(ValidationResult::Pass)
    }
}
