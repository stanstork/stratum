use model::{
    core::value::Value, records::row::RowData, transform::mapping::TransformationMetadata,
};
use std::collections::HashMap;

/// Function type for getting environment variables
pub type EnvGetter = fn(&str) -> Option<String>;

/// Evaluation context that provides access to different data sources
/// depending on whether we're evaluating at build-time or runtime
pub enum EvalContext<'a> {
    /// Build-time evaluation: has access to global definitions and env getter
    BuildTime {
        definitions: &'a HashMap<String, Value>,
        env_getter: EnvGetter,
    },
    /// Runtime evaluation: has access to row data, mappings, and env getter
    Runtime {
        row_data: &'a RowData,
        mapping: &'a TransformationMetadata,
        env_getter: EnvGetter,
    },
}

impl<'a> EvalContext<'a> {
    pub fn get_env(&self, key: &str) -> Option<String> {
        match self {
            EvalContext::BuildTime { env_getter, .. } => env_getter(key),
            EvalContext::Runtime { env_getter, .. } => env_getter(key),
        }
    }

    pub fn get_env_or(&self, key: &str, default: &str) -> String {
        self.get_env(key).unwrap_or_else(|| default.to_string())
    }

    /// Get global definition (build-time only)
    pub fn get_definition(&self, key: &str) -> Option<&Value> {
        match self {
            EvalContext::BuildTime { definitions, .. } => definitions.get(key),
            EvalContext::Runtime { .. } => None,
        }
    }

    /// Get row data (runtime only)
    pub fn get_row_data(&self) -> Option<&RowData> {
        match self {
            EvalContext::BuildTime { .. } => None,
            EvalContext::Runtime { row_data, .. } => Some(row_data),
        }
    }

    /// Get transformation mapping (runtime only)
    pub fn get_mapping(&self) -> Option<&TransformationMetadata> {
        match self {
            EvalContext::BuildTime { .. } => None,
            EvalContext::Runtime { mapping, .. } => Some(mapping),
        }
    }
}
