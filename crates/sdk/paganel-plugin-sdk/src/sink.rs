use crate::{
    error::{PluginError, PluginResult},
    record::Record,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Default)]
pub struct SinkConfig {
    params: HashMap<String, String>,
}

impl SinkConfig {
    pub fn new(params: HashMap<String, String>) -> Self {
        Self { params }
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.params.get(key).map(String::as_str)
    }

    pub fn require(&self, key: &str) -> PluginResult<&str> {
        self.get(key)
            .ok_or_else(|| PluginError::invalid_input(format!("missing config key: '{}'", key)))
    }
}

#[derive(Debug, Clone, Default)]
pub struct PluginBatch {
    pub records: Vec<Record>,
}

impl PluginBatch {
    pub fn from_json_bytes(bytes: &[u8]) -> PluginResult<Self> {
        let json: serde_json::Value = serde_json::from_slice(bytes)?;
        let arr = json
            .get("records")
            .and_then(|v| v.as_array())
            .ok_or_else(|| PluginError::invalid_input("missing 'records' array"))?;
        let mut records = Vec::with_capacity(arr.len());
        for (i, row) in arr.iter().enumerate() {
            let obj = row.as_object().ok_or_else(|| {
                PluginError::invalid_input(format!("record {} is not an object", i))
            })?;
            records.push(Record::from_json_object(obj)?);
        }
        Ok(Self { records })
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteResult {
    pub rows_written: u64,
}

impl WriteResult {
    pub fn new(rows_written: u64) -> Self {
        Self { rows_written }
    }

    pub fn to_json_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }
}
