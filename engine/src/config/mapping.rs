use crate::transform::mapping::TransformMapping;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMapping {
    pub table: String,
    #[serde(default, deserialize_with = "deserialize_columns")]
    pub columns: Option<HashMap<String, String>>,
    #[serde(default)]
    pub transform: Option<Vec<TransformMapping>>,
    #[serde(default)]
    pub infer_schema: bool,
}

fn deserialize_columns<'de, D>(deserializer: D) -> Result<Option<HashMap<String, String>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw: HashMap<String, String> = HashMap::deserialize(deserializer)?;
    let columns = raw
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();

    Ok(Some(columns))
}
