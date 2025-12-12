use crate::{error::TransformError, transform::pipeline::Validator};
use engine_core::context::env::get_env;
use expression_engine::eval::runtime::Evaluator;
use model::{
    core::value::Value, execution::pipeline::ValidationRule, records::row::RowData,
    transform::mapping::TransformationMetadata,
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

/// Validator that evaluates validation rules from a Pipeline
pub struct PipelineValidator {
    rules: Vec<ValidationRule>,
    metadata: TransformationMetadata,
}

impl PipelineValidator {
    pub fn new(rules: Vec<ValidationRule>, metadata: TransformationMetadata) -> Self {
        Self { rules, metadata }
    }
}

impl Validator for PipelineValidator {
    fn validate(&self, row: &RowData) -> Result<ValidationResult, TransformError> {
        for rule in &self.rules {
            // Evaluate the validation check expression
            let result = rule.check.evaluate(row, &self.metadata, get_env);

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
