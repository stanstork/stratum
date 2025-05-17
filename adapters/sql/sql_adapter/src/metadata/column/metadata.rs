use crate::row::db_row::DbRow;
use bigdecimal::ToPrimitive;
use common::{types::DataType, value::Value};

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

pub const COL_REFERENCING_TABLE: &str = "referencing_table";

#[derive(Debug, Clone)]
pub struct ColumnMetadata {
    pub ordinal: usize,
    pub name: String,
    pub data_type: DataType,
    pub is_nullable: bool,
    pub has_default: bool,
    pub default_value: Option<Value>,
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

impl ColumnMetadata {
    pub fn from_row(row: &DbRow, data_type: DataType) -> Self {
        Self {
            ordinal: row.try_get_u32(COL_ORDINAL_POSITION).unwrap_or(0) as usize,
            name: row.try_get_string(COL_COLUMN_NAME).unwrap_or_default(),
            data_type,
            is_nullable: row.try_get_string(COL_IS_NULLABLE).unwrap_or_default() == "YES",
            has_default: row.try_get_string(COL_COLUMN_DEFAULT).is_some(),
            default_value: from_row(row, data_type, COL_COLUMN_DEFAULT),
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

pub fn from_row(row: &DbRow, data_type: DataType, name: &str) -> Option<Value> {
    match data_type {
        DataType::Int | DataType::Long | DataType::Short => row.try_get_i64(name).map(Value::Int),
        DataType::IntUnsigned | DataType::ShortUnsigned | DataType::Year => {
            row.try_get_u64(name).map(|v| Value::Int(v as i64))
        }
        DataType::Float => row.try_get_f64(name).map(Value::Float),
        DataType::Decimal => row
            .try_get_bigdecimal(name)
            .and_then(|v| v.to_f64().map(Value::Float)),
        DataType::String | DataType::VarChar | DataType::Char => {
            row.try_get_string(name).map(Value::String)
        }
        DataType::Boolean => row.try_get_bool(name).map(Value::Boolean),
        DataType::Json => row.try_get_json(name).map(Value::Json),
        DataType::Timestamp => row.try_get_timestamp(name).map(Value::Timestamp),
        DataType::Date => row.try_get_date(name).map(Value::Date),
        DataType::Enum => row.try_get_string(name).map(Value::String),
        DataType::Bytea => row.try_get_bytes(name).map(Value::Bytes),
        DataType::Blob | DataType::TinyBlob | DataType::MediumBlob | DataType::LongBlob => {
            row.try_get_bytes(name).map(Value::Bytes)
        }
        _ => None,
    }
}
