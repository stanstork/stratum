use crate::{exchange::json_v1, value::Value};

/// A single typed value returned by a transform plugin.
#[derive(Debug, Clone)]
pub struct PluginOutput {
    pub value: Value,
}

impl PluginOutput {
    pub fn new(value: impl Into<Value>) -> Self {
        Self {
            value: value.into(),
        }
    }

    pub fn null() -> Self {
        Self { value: Value::Null }
    }

    /// Serialize as the host-compatible `{"type": ..., "value": ...}` object.
    pub fn to_json_bytes(&self) -> Vec<u8> {
        let json = json_v1::value_to_json(&self.value);
        serde_json::to_vec(&json).unwrap_or_default()
    }
}

impl<T: Into<Value>> From<T> for PluginOutput {
    fn from(v: T) -> Self {
        PluginOutput::new(v)
    }
}
