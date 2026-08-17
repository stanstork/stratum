use crate::transform::{
    error::TransformError,
    pipeline::{Validator, for_each_table},
};
use engine_core::context::env::EnvContext;
use engine_wasm::{
    exchange::types::{FilterDecision, PluginInput},
    registry::PluginRegistry,
    runtime::instance::PluginInstance,
};
use expression_engine::{EvalContext, Evaluator, Program, TreeExpr};
use model::{
    core::value::Value,
    execution::{
        expr::CompiledExpression,
        pipeline::{ValidationAction, ValidationKind, ValidationRule},
    },
    records::Record,
    transform::mapping::TransformationMetadata,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Skip, // Filter out the row
    Fail, // Stop the pipeline
    Warn, // Log warning but continue
}

pub enum ValidationResult {
    Pass,
    Failed {
        rule: String,
        message: String,
        action: Action,
    },
}

/// Executable state for a validation rule, resolved once at construction.
enum RuleExecutor {
    Assert(CompiledExpression),
    WasmFilter {
        plugin: Box<Mutex<PluginInstance>>,
        plugin_name: String,
        input_mapping: HashMap<String, String>,
    },
}

/// An assert expression compiled for one table run.
enum Compiled {
    Vm(Program),
    Tree(TreeExpr),
}

/// Interpret an assert expression's result.
fn interpret_assert(result: Option<Value>, rule_label: &str) -> Result<bool, TransformError> {
    match result {
        Some(Value::Boolean(b)) => Ok(b),
        Some(Value::Null) => Ok(false),
        Some(_) => Err(TransformError::Transformation(format!(
            "Validation rule '{rule_label}' returned non-boolean value"
        ))),
        None => Ok(false),
    }
}

/// A pre-compiled validation rule: its metadata plus its executable state.
struct ActiveRule {
    label: String,
    message: String,
    action: Action,
    executor: RuleExecutor,
}

impl ActiveRule {
    /// Build the `Failed` result for this rule.
    fn failed(&self, reject_reason: Option<String>) -> ValidationResult {
        ValidationResult::Failed {
            rule: self.label.clone(),
            message: reject_reason.unwrap_or_else(|| self.message.clone()),
            action: self.action,
        }
    }
}

/// Validator that evaluates validation rules from a Pipeline. Supports both
/// expression-based asserts and WASM filter plugins.
pub struct PipelineValidator {
    rules: Vec<ActiveRule>,
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
        let rules = rules
            .into_iter()
            .map(|rule| {
                let action = match rule.action {
                    ValidationAction::Skip => Action::Skip,
                    ValidationAction::Fail => Action::Fail,
                    ValidationAction::Warn | ValidationAction::Continue => Action::Warn,
                };

                let executor = match rule.kind {
                    ValidationKind::Assert { check } => RuleExecutor::Assert(check),
                    ValidationKind::WasmFilter {
                        plugin_name,
                        input_mapping,
                    } => {
                        let plugin = plugin_registry.instantiate(&plugin_name).map_err(|e| {
                            TransformError::Transformation(format!(
                                "validation plugin '{plugin_name}' instantiation failed: {e}"
                            ))
                        })?;

                        RuleExecutor::WasmFilter {
                            plugin: Box::new(Mutex::new(plugin)),
                            plugin_name,
                            input_mapping,
                        }
                    }
                };

                Ok(ActiveRule {
                    label: rule.label,
                    message: rule.message,
                    action,
                    executor,
                })
            })
            .collect::<Result<Vec<_>, TransformError>>()?;

        Ok(Self {
            rules,
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
        let env_getter = |key: &str| self.env.get(key);
        interpret_assert(check.evaluate(row, &self.metadata, &env_getter), rule_label)
    }

    fn evaluate_wasm(
        plugin: &Mutex<PluginInstance>,
        plugin_name: &str,
        input_mapping: &HashMap<String, String>,
        row: &Record,
    ) -> Result<(bool, Option<String>), TransformError> {
        let input = PluginInput::from_record(row, input_mapping);
        let mut guard = plugin.lock().expect("validation plugin mutex poisoned");

        let decision = guard
            .call_evaluate(&[input])
            .map_err(|e| {
                TransformError::Transformation(format!("wasm filter '{plugin_name}' failed: {e}"))
            })?
            .into_iter()
            .next()
            .ok_or_else(|| {
                TransformError::Transformation(format!(
                    "wasm filter '{plugin_name}' returned no decision"
                ))
            })?;

        Ok(match decision {
            FilterDecision::Pass => (true, None),
            FilterDecision::Reject { reason } => (false, Some(reason)),
        })
    }

    /// Evaluate a WASM filter rule over a whole batch in one crossing.
    fn evaluate_wasm_batch(
        plugin: &Mutex<PluginInstance>,
        plugin_name: &str,
        input_mapping: &HashMap<String, String>,
        rows: &[Record],
    ) -> Result<Vec<FilterDecision>, TransformError> {
        let mut guard = plugin.lock().expect("validation plugin mutex poisoned");
        guard
            .call_evaluate_records(rows, input_mapping)
            .map_err(|e| {
                TransformError::Transformation(format!("wasm filter '{plugin_name}' failed: {e}"))
            })
    }
}

impl Validator for PipelineValidator {
    fn validate(&self, row: &Record) -> Result<ValidationResult, TransformError> {
        for rule in &self.rules {
            let (passed, reject_reason) = match &rule.executor {
                RuleExecutor::Assert(check) => {
                    (self.evaluate_assert(check, &rule.label, row)?, None)
                }
                RuleExecutor::WasmFilter {
                    plugin,
                    plugin_name,
                    input_mapping,
                } => Self::evaluate_wasm(plugin, plugin_name, input_mapping, row)?,
            };

            if !passed {
                return Ok(rule.failed(reject_reason));
            }
        }

        Ok(ValidationResult::Pass)
    }

    /// Batch-native validation. Rules run in order across the whole batch.
    fn validate_batch(
        &self,
        rows: &[Record],
        out: &mut Vec<Result<ValidationResult, TransformError>>,
    ) {
        // `None` = still passing every rule evaluated so far.
        let mut decided: Vec<Option<Result<ValidationResult, TransformError>>> =
            std::iter::repeat_with(|| None).take(rows.len()).collect();

        for rule in &self.rules {
            match &rule.executor {
                RuleExecutor::Assert(check) => {
                    let env_getter = |key: &str| self.env.get(key);

                    for_each_table(rows, |offset, run| {
                        let Some(first) = run.first() else {
                            return;
                        };
                        let schema = first.schema();
                        let table = schema.table();

                        // Idiomatic fallback if VM compilation fails
                        let compiled = Program::compile(check, schema, &self.metadata, table)
                            .map(Compiled::Vm)
                            .unwrap_or_else(|| {
                                Compiled::Tree(TreeExpr::compile(
                                    check,
                                    schema,
                                    &self.metadata,
                                    table,
                                ))
                            });

                        for (i, row) in run.iter().enumerate() {
                            let slot = &mut decided[offset + i];

                            // Skip if this row has already failed a prior rule
                            if slot.is_some() {
                                continue;
                            }

                            let value = match &compiled {
                                Compiled::Vm(p) => {
                                    let ctx = EvalContext::Runtime {
                                        row_data: row,
                                        mapping: &self.metadata,
                                        env_getter: &env_getter,
                                    };
                                    p.eval(row, &ctx)
                                }
                                Compiled::Tree(e) => e.eval(row, &self.metadata, &env_getter),
                            }
                            .map(|c| c.into_owned());

                            match interpret_assert(value, &rule.label) {
                                Ok(true) => {} // Still passing
                                Ok(false) => *slot = Some(Ok(rule.failed(None))),
                                Err(e) => *slot = Some(Err(e)),
                            }
                        }
                    });
                }
                RuleExecutor::WasmFilter {
                    plugin,
                    plugin_name,
                    input_mapping,
                } => {
                    match Self::evaluate_wasm_batch(plugin, plugin_name, input_mapping, rows) {
                        Ok(decisions) => {
                            for (slot, decision) in decided.iter_mut().zip(decisions) {
                                if slot.is_none()
                                    && let FilterDecision::Reject { reason } = decision
                                {
                                    *slot = Some(Ok(rule.failed(Some(reason))));
                                }
                            }
                        }
                        Err(e) => {
                            // A crossing failure is fatal for the run.
                            let msg = e.to_string();
                            for slot in decided.iter_mut().filter(|s| s.is_none()) {
                                *slot = Some(Err(TransformError::Transformation(msg.clone())));
                            }
                        }
                    }
                }
            }
        }

        out.extend(
            decided
                .into_iter()
                .map(|slot| slot.unwrap_or(Ok(ValidationResult::Pass))),
        );
    }
}
