use serde::{Serialize, Serializer};
use std::fmt::Display;

#[derive(Debug, Clone)]
pub struct ValidationCheck {
    pub expression: String,
    pub columns_referenced: Vec<String>,
}

impl Serialize for ValidationCheck {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Serialize as just the expression string
        serializer.serialize_str(&self.expression)
    }
}

impl Display for ValidationCheck {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.expression)
    }
}

#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ValidationLevel {
    Assert,
    Warn,
}

#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ValidationAction {
    Skip,
    Fail,
}
