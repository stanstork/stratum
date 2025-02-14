use crate::transform::mapping::TransformMapping;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMapping {
    pub table: String,
    #[serde(deserialize_with = "deserialize_columns")]
    pub columns: HashMap<String, String>,
    pub transform: Vec<TransformMapping>,
}

fn deserialize_columns<'de, D>(deserializer: D) -> Result<HashMap<String, String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw: HashMap<String, String> = HashMap::deserialize(deserializer)?;
    Ok(raw.into_iter().map(|(k, v)| (k, v.to_string())).collect())
}
