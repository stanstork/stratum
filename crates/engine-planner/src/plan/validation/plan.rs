use crate::plan::validation::types::{ValidationAction, ValidationCheck, ValidationLevel};
use serde::Serialize;

#[derive(Serialize, Debug, Clone)]
pub struct ValidationPlan {
    pub name: String,

    /// Type: assert or warn
    pub level: ValidationLevel,

    /// Check expression
    pub check: ValidationCheck,

    /// Message
    pub message: String,

    /// Action when fails (assert only)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action: Option<ValidationAction>,

    /// Estimated percentage of rows that will fail (0.0 to 1.0)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_failure_rate: Option<f32>,
}
