use super::{data_type::ColumnDataType, value::ColumnValue};
use crate::row::db_row::DbRow;
use serde::{Deserialize, Serialize};

const COL_ORDINAL_POSITION: &str = "ordinal_position";
const COL_COLUMN_NAME: &str = "column_name";
const COL_IS_NULLABLE: &str = "is_nullable";
const COL_COLUMN_DEFAULT: &str = "column_default";
const COL_CHAR_MAX_LENGTH: &str = "character_maximum_length";
const COL_NUMERIC_PRECISION: &str = "numeric_precision";
const COL_NUMERIC_SCALE: &str = "numeric_scale";
const COL_IS_PRIMARY_KEY: &str = "is_primary_key";
const COL_IS_UNIQUE: &str = "is_unique";
const COL_IS_AUTO_INCREMENT: &str = "is_auto_increment";
const COL_REFERENCED_TABLE: &str = "referenced_table";
const COL_REFERENCED_COLUMN: &str = "referenced_column";
const COL_ON_DELETE: &str = "on_delete";
const COL_ON_UPDATE: &str = "on_update";

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

impl From<&DbRow<'_>> for ColumnMetadata {
    fn from(row: &DbRow) -> Self {
        let data_type = ColumnDataType::from_row(row);
        Self {
            ordinal: row.try_get_i32(COL_ORDINAL_POSITION).unwrap_or(0) as usize,
            name: row.try_get_string(COL_COLUMN_NAME).unwrap_or_default(),
            data_type,
            is_nullable: row.try_get_string(COL_IS_NULLABLE).unwrap_or_default() == "YES",
            has_default: row.try_get_string(COL_COLUMN_DEFAULT).is_some(),
            default_value: ColumnValue::from_row(row, data_type, COL_COLUMN_DEFAULT),
            char_max_length: row.try_get_i64(COL_CHAR_MAX_LENGTH).map(|v| v as usize),
            num_precision: row.try_get_i32(COL_NUMERIC_PRECISION).map(|v| v as u32),
            num_scale: row.try_get_i32(COL_NUMERIC_SCALE).map(|v| v as u32),
            is_primary_key: row.try_get_bool(COL_IS_PRIMARY_KEY).unwrap_or(false),
            is_unique: row.try_get_bool(COL_IS_UNIQUE).unwrap_or(false),
            is_auto_increment: row.try_get_bool(COL_IS_AUTO_INCREMENT).unwrap_or(false),
            referenced_table: row.try_get_string(COL_REFERENCED_TABLE),
            referenced_column: row.try_get_string(COL_REFERENCED_COLUMN),
            on_delete: row.try_get_string(COL_ON_DELETE),
            on_update: row.try_get_string(COL_ON_UPDATE),
        }
    }
}
