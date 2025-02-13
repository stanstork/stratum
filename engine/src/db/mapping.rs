use super::{col::ColumnType, transform::Transformation};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMapping {
    pub table: String,
    #[serde(deserialize_with = "deserialize_columns")]
    pub columns: HashMap<String, ColumnMapping>,
    pub transform: Vec<Transformation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMapping {
    pub column: String,
    pub target_type: Option<ColumnType>,
}

impl From<String> for ColumnMapping {
    fn from(column: String) -> Self {
        let parts = column.split("::").collect::<Vec<&str>>();
        let column = parts[0].to_string();
        let target_type = parts.get(1).map(|t| ColumnType::from(*t));
        Self {
            column,
            target_type,
        }
    }
}

fn deserialize_columns<'de, D>(deserializer: D) -> Result<HashMap<String, ColumnMapping>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw: HashMap<String, String> = HashMap::deserialize(deserializer)?;
    Ok(raw
        .into_iter()
        .map(|(k, v)| (k, ColumnMapping::from(v)))
        .collect())
}
