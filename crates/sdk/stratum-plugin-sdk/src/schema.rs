use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginType {
    Transform,
    Filter,
    Source,
    Sink,
}

/// One field in a plugin's declared input or output schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginField {
    pub name: String,
    #[serde(rename = "type")]
    pub field_type: String,
    #[serde(default)]
    pub nullable: bool,
}

/// The metadata document emitted by `__stratum_metadata`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginMetadata {
    pub name: String,
    pub version: String,
    #[serde(rename = "type")]
    pub plugin_type: PluginType,
    #[serde(default = "default_exchange_format")]
    pub exchange_format: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub input_schema: Vec<PluginField>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub output_schema: Vec<PluginField>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_type: Option<String>,
}

fn default_exchange_format() -> String {
    "json_v1".into()
}
