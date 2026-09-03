use crate::value::Value;

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
}

impl<T: Into<Value>> From<T> for PluginOutput {
    fn from(v: T) -> Self {
        PluginOutput::new(v)
    }
}
