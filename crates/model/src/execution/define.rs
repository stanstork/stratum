use crate::core::value::Value;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Definition with its evaluated value and source information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DefinitionInfo {
    pub value: Value,
    pub source: DefinitionSource,
}

/// Global definitions from define {} block
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalDefinitions {
    pub variables: HashMap<String, DefinitionInfo>,
}

/// Tracks the source of a definition value for planner reporting
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DefinitionSource {
    /// Literal value in SMQL config
    Literal,

    /// From env("VAR") - required
    Environment { var_name: String },

    /// From env("VAR", default) - with fallback
    EnvironmentWithDefault {
        var_name: String,
        default_value: String,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EnvVar {
    pub var_name: String,
    pub was_set: bool,
    pub used_default: bool,
    pub value: Value,
}
