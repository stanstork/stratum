use crate::{
    core::value::{FieldValue, Value},
    records::record::DataRecord,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowData {
    pub entity: String,
    pub field_values: Vec<FieldValue>,
}

impl RowData {
    pub fn new(entity: &str, field_values: Vec<FieldValue>) -> Self {
        RowData {
            entity: entity.to_string(),
            field_values,
        }
    }

    pub fn get(&self, field: &str) -> Option<&FieldValue> {
        self.field_values
            .iter()
            .find(|f| f.name.eq_ignore_ascii_case(field))
    }

    pub fn get_value(&self, field: &str) -> Value {
        self.get(field)
            .and_then(|f| f.value.clone())
            .unwrap_or(Value::Null)
    }

    pub fn entity_name(&self) -> String {
        self.entity.clone()
    }

    pub fn encode_csv_value(&self, field: &str) -> String {
        let value = self
            .field_values
            .iter()
            .find(|f| f.name.eq_ignore_ascii_case(field))
            .and_then(|f| f.value.as_ref());

        if let Some(v) = value {
            v.encode_csv()
        } else {
            "\\N".to_string()
        }
    }

    pub fn size_bytes(&self) -> usize {
        let mut size = self.entity.len();
        for field_value in &self.field_values {
            size += field_value.name.len();
            size += match &field_value.value {
                Some(v) => v.size_bytes(),
                None => 0,
            };
        }
        size
    }
}

impl DataRecord for RowData {
    fn debug(&self) {
        println!("{self:#?}");
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn serialize(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_else(|_| {
            panic!("Failed to serialize: {self:?}");
        })
    }

    fn deserialize(data: Vec<u8>) -> Self {
        serde_json::from_slice(&data).unwrap_or_else(|_| {
            panic!("Failed to deserialize: {data:?}");
        })
    }
}
