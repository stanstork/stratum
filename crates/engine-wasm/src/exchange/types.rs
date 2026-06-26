use model::{core::value::Value, records::Record};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Input to a transform or filter plugin call.
/// Named fields with typed values extracted from the current row.
#[derive(Debug, Clone)]
pub struct PluginInput {
    fields: HashMap<String, Value>,
}

impl Default for PluginInput {
    fn default() -> Self {
        Self::new()
    }
}

impl PluginInput {
    pub fn new() -> Self {
        Self {
            fields: HashMap::new(),
        }
    }

    pub fn insert(&mut self, name: String, value: Value) {
        self.fields.insert(name, value);
    }

    /// Build from a Record using an input mapping.
    /// `mapping` maps plugin_field_name -> source_field_name.
    pub fn from_record(record: &Record, mapping: &HashMap<String, String>) -> Self {
        let mut input = Self::new();
        for (plugin_field, source_field) in mapping {
            let value = record.get_value(source_field);
            input.insert(plugin_field.clone(), value);
        }
        input
    }

    pub fn get(&self, name: &str) -> Option<&Value> {
        self.fields.get(name)
    }

    pub fn fields(&self) -> &HashMap<String, Value> {
        &self.fields
    }
}

/// Output from a transform plugin. A single typed value.
#[derive(Debug, Clone)]
pub struct PluginOutput {
    pub value: Value,
}

/// Output from a filter plugin.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FilterDecision {
    Pass,
    Reject { reason: String },
}

impl FilterDecision {
    pub fn pass() -> Self {
        Self::Pass
    }

    pub fn reject(reason: impl Into<String>) -> Self {
        Self::Reject {
            reason: reason.into(),
        }
    }

    pub fn is_pass(&self) -> bool {
        matches!(self, Self::Pass)
    }
}

/// A page of records returned by a source plugin.
#[derive(Debug, Clone)]
pub struct SourcePage {
    pub records: Vec<Record>,
    pub next_cursor: Option<String>,
    pub has_more: bool,
}

/// A batch of records passed to a sink plugin.
#[derive(Debug, Clone)]
pub struct PluginBatch {
    pub records: Vec<Record>,
}

/// Result of a sink write_batch call.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteResult {
    pub rows_written: u64,
}
