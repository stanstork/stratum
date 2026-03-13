use crate::core::value::{FieldValue, Value};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub mod batch;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    pub schema: String,
    pub fields: Vec<FieldValue>,
}

impl Record {
    pub fn new(schema: &str, fields: Vec<FieldValue>) -> Self {
        Record {
            schema: schema.to_string(),
            fields,
        }
    }

    pub fn get(&self, field: &str) -> Option<&FieldValue> {
        self.fields
            .iter()
            .find(|f| f.name.eq_ignore_ascii_case(field))
    }

    pub fn get_value(&self, field: &str) -> Value {
        self.get(field)
            .and_then(|f| f.value.clone())
            .unwrap_or(Value::Null)
    }

    pub fn schema(&self) -> String {
        self.schema.clone()
    }

    pub fn size_bytes(&self) -> usize {
        let mut size = self.schema.len();
        for field_value in &self.fields {
            size += field_value.name.len();
            size += match &field_value.value {
                Some(v) => v.size_bytes(),
                None => 0,
            };
        }
        size
    }

    pub fn to_map(&self) -> HashMap<String, Value> {
        self.fields
            .iter()
            .filter_map(|fv| fv.value.as_ref().map(|v| (fv.name.clone(), v.clone())))
            .collect()
    }
}
