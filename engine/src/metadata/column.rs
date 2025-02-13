use crate::database::column::ColumnType;
use serde::{Deserialize, Serialize};
use sqlx::mysql::MySqlColumn;
use sqlx::{Column, TypeInfo};

#[derive(Debug, Serialize, Deserialize)]
pub struct ColumnMetadata {
    pub ordinal: usize,
    pub name: String,
    pub type_info: ColumnType,
}

impl From<&MySqlColumn> for ColumnMetadata {
    fn from(column: &MySqlColumn) -> Self {
        Self {
            ordinal: column.ordinal(),
            name: column.name().to_string(),
            type_info: ColumnType::from(column.type_info().name()),
        }
    }
}
