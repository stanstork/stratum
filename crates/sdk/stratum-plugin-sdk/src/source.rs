use std::collections::HashMap;

use crate::{
    error::{PluginError, PluginResult},
    exchange::json_v1,
    record::Record,
};

/// Static configuration delivered at plugin initialize time.
#[derive(Debug, Clone, Default)]
pub struct SourceConfig {
    params: HashMap<String, String>,
}

impl SourceConfig {
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

/// A page of records produced by a source plugin.
#[derive(Debug, Clone, Default)]
pub struct SourcePage {
    pub records: Vec<Record>,
    pub next_cursor: Option<String>,
    pub has_more: bool,
}

impl SourcePage {
    pub fn empty() -> Self {
        Self::default()
    }

    /// Wire form matches engine-wasm's deserialize_source_page.
    pub fn to_json_bytes(&self) -> Vec<u8> {
        let records: Vec<serde_json::Value> = self
            .records
            .iter()
            .map(|r| {
                let mut map = serde_json::Map::with_capacity(r.fields.len());
                for f in &r.fields {
                    map.insert(f.name.clone(), json_v1::value_to_json(&f.value));
                }
                serde_json::Value::Object(map)
            })
            .collect();

        serde_json::to_vec(&serde_json::json!({
            "records": records,
            "next_cursor": self.next_cursor,
            "has_more": self.has_more,
        }))
        .unwrap_or_default()
    }
}
