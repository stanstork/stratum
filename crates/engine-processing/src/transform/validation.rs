use crate::transform::{error::TransformError, pipeline::Validator};
use engine_core::context::env::EnvContext;
use expression_engine::eval::runtime::Evaluator;
use model::{
    core::value::Value, execution::pipeline::ValidationRule, records::Record,
    transform::mapping::TransformationMetadata,
};
use std::sync::Arc;

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

/// Validator that evaluates validation rules from a Pipeline
pub struct PipelineValidator {
    rules: Vec<ValidationRule>,
    metadata: TransformationMetadata,
    env: Arc<EnvContext>,
}

impl PipelineValidator {
    pub fn new(
        rules: Vec<ValidationRule>,
        metadata: TransformationMetadata,
        env: Arc<EnvContext>,
    ) -> Self {
        Self {
            rules,
            metadata,
            env,
        }
    }
}

impl Validator for PipelineValidator {
    fn validate(&self, row: &Record) -> Result<ValidationResult, TransformError> {
        let env = self.env.clone();
        let env_getter = move |key: &str| env.get(key);
        for rule in &self.rules {
            // Evaluate the validation check expression
            let result = rule.check.evaluate(row, &self.metadata, &env_getter);

            // Check if the validation passed (expression should evaluate to true)
            let passed = match result {
                Some(Value::Boolean(true)) => true,
                Some(Value::Boolean(false)) => false,
                Some(_) => {
                    // Non-boolean result is treated as validation error
                    return Err(TransformError::Transformation(format!(
                        "Validation rule '{}' returned non-boolean value",
                        rule.label
                    )));
                }
                None => {
                    // Null/missing value is treated as failed validation
                    false
                }
            };

            if !passed {
                // Convert model ValidationAction to processing ValidationAction
                let action = match rule.action {
                    model::execution::pipeline::ValidationAction::Skip => ValidationAction::Skip,
                    model::execution::pipeline::ValidationAction::Fail => ValidationAction::Fail,
                    model::execution::pipeline::ValidationAction::Warn => ValidationAction::Warn,
                    model::execution::pipeline::ValidationAction::Continue => {
                        ValidationAction::Warn
                    }
                };

                return Ok(ValidationResult::Failed {
                    rule: rule.label.clone(),
                    message: rule.message.clone(),
                    action,
                });
            }
        }

        Ok(ValidationResult::Pass)
    }
}
