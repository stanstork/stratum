use super::{data_type::ColumnDataType, value::ColumnValue};
use serde::{Deserialize, Serialize};
use sqlx::mysql::MySqlRow;
use sqlx::postgres::PgRow;
use sqlx::Row;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMetadata {
    pub ordinal: usize,
    pub name: String,
    pub data_type: ColumnDataType,
    pub is_nullable: bool,
    pub has_default: bool,
    pub default_value: Option<ColumnValue>,
    pub char_max_length: Option<usize>,
    pub num_precision: Option<u32>,
    pub num_scale: Option<u32>,
    pub is_primary_key: bool,
    pub is_unique: bool,
    pub is_auto_increment: bool,
    pub referenced_table: Option<String>,
    pub referenced_column: Option<String>,
    pub on_delete: Option<String>,
    pub on_update: Option<String>,
}

impl From<&MySqlRow> for ColumnMetadata {
    fn from(row: &MySqlRow) -> Self {
        let data_type = ColumnDataType::from_mysql_row(row);
        Self {
            ordinal: row.try_get::<i32, _>("ORDINAL_POSITION").unwrap_or(0) as usize,
            name: row.try_get::<String, _>("COLUMN_NAME").unwrap_or_default(),
            data_type,
            is_nullable: row.try_get::<i32, _>("IS_NULLABLE").unwrap_or(0) == 1,
            has_default: row.try_get::<i32, _>("HAS_DEFAULT").unwrap_or(0) == 1,
            default_value: ColumnValue::from_mysql_row(row, data_type, "COLUMN_DEFAULT"),
            char_max_length: row
                .try_get::<i64, _>("CHARACTER_MAXIMUM_LENGTH")
                .ok()
                .map(|v| v as usize),
            num_precision: row.try_get::<u32, _>("NUMERIC_PRECISION").ok(),
            num_scale: row.try_get::<u32, _>("NUMERIC_SCALE").ok(),
            is_primary_key: row.try_get::<i32, _>("IS_PRIMARY_KEY").unwrap_or(0) == 1,
            is_unique: row.try_get::<i32, _>("IS_UNIQUE").unwrap_or(0) == 1,
            is_auto_increment: row.try_get::<i32, _>("IS_AUTO_INCREMENT").unwrap_or(0) == 1,
            referenced_table: row.try_get::<String, _>("REFERENCED_TABLE").ok(),
            referenced_column: row.try_get::<String, _>("REFERENCED_COLUMN").ok(),
            on_delete: row.try_get::<String, _>("ON_DELETE").ok(),
            on_update: row.try_get::<String, _>("ON_UPDATE").ok(),
        }
    }
}

impl From<&PgRow> for ColumnMetadata {
    fn from(row: &PgRow) -> Self {
        let data_type = ColumnDataType::from_pg_row(row);
        Self {
            ordinal: row.try_get::<i32, _>("ordinal_position").unwrap_or(0) as usize,
            name: row.try_get::<String, _>("column_name").unwrap_or_default(),
            data_type,
            is_nullable: row.try_get::<String, _>("is_nullable").unwrap_or_default() == "YES",
            has_default: row.try_get::<String, _>("column_default").is_ok(),
            default_value: ColumnValue::from_pg_row(row, data_type, "column_default"),
            char_max_length: row
                .try_get::<i64, _>("character_maximum_length")
                .ok()
                .map(|v| v as usize),
            num_precision: row
                .try_get::<i32, _>("numeric_precision")
                .ok()
                .map(|v| v as u32),
            num_scale: row
                .try_get::<i32, _>("numeric_scale")
                .ok()
                .map(|v| v as u32),
            is_primary_key: row.try_get::<bool, _>("is_primary_key").unwrap_or(false),
            is_unique: row.try_get::<bool, _>("is_unique").unwrap_or(false),
            is_auto_increment: row.try_get::<bool, _>("is_auto_increment").unwrap_or(false),
            referenced_table: row.try_get::<String, _>("referenced_table").ok(),
            referenced_column: row.try_get::<String, _>("referenced_column").ok(),
            on_delete: row.try_get::<String, _>("on_delete").ok(),
            on_update: row.try_get::<String, _>("on_update").ok(),
        }
    }
}
