use crate::{error::PluginResult, exchange::json_v1, value::Value};
use serde_json::Value as JsonValue;

/// A single named value within a record.
#[derive(Debug, Clone)]
pub struct FieldValue {
    pub name: String,
    pub value: Value,
}

/// Guest-side row. Source plugins build Records to emit; sink plugins read them.
#[derive(Debug, Clone, Default)]
pub struct Record {
    pub fields: Vec<FieldValue>,
}

impl Record {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(n: usize) -> Self {
        Self {
            fields: Vec::with_capacity(n),
        }
    }

    pub fn set(&mut self, name: impl Into<String>, value: impl Into<Value>) -> &mut Self {
        self.fields.push(FieldValue {
            name: name.into(),
            value: value.into(),
        });
        self
    }

    pub fn get(&self, name: &str) -> Option<&Value> {
        self.fields
            .iter()
            .find(|f| f.name == name)
            .map(|f| &f.value)
    }

    /// Wire form: `{ "col_a": {"type": "...", "value": ...}, "col_b": {...} }`
    pub fn to_json_bytes(&self) -> Vec<u8> {
        let mut map = serde_json::Map::with_capacity(self.fields.len());
        for f in &self.fields {
            map.insert(f.name.clone(), json_v1::value_to_json(&f.value));
        }
        serde_json::to_vec(&JsonValue::Object(map)).unwrap_or_default()
    }

    pub fn from_json_object(obj: &serde_json::Map<String, JsonValue>) -> PluginResult<Self> {
        let mut fields = Vec::with_capacity(obj.len());
        for (name, typed_val) in obj {
            let value = json_v1::json_to_value(typed_val)?;
            fields.push(FieldValue {
                name: name.clone(),
                value,
            });
        }
        Ok(Self { fields })
    }
}
