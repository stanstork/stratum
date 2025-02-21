use bigdecimal::{BigDecimal, ToPrimitive};
use chrono::{NaiveDate, NaiveDateTime};
use core::fmt;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::mysql::MySqlRow;
use sqlx::Row;
use std::collections::HashMap;
use std::convert::TryFrom;
use uuid::Uuid;

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
        let data_type = ColumnDataType::from_row(row);
        Self {
            ordinal: row.try_get::<i32, _>("ORDINAL_POSITION").unwrap_or(0) as usize,
            name: row.try_get::<String, _>("COLUMN_NAME").unwrap_or_default(),
            data_type,
            is_nullable: row.try_get::<i32, _>("IS_NULLABLE").unwrap_or(0) == 1,
            has_default: row.try_get::<i32, _>("HAS_DEFAULT").unwrap_or(0) == 1,
            default_value: ColumnValue::from_row(row, data_type, "COLUMN_DEFAULT"),
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ColumnDataType {
    Decimal,
    Tiny,
    Short,
    Long,
    Float,
    Double,
    Null,
    Timestamp,
    LongLong,
    Int24,
    Date,
    Time,
    Datetime,
    Year,
    VarChar,
    Bit,
    Json,
    NewDecimal,
    Enum,
    Set,
    TinyBlob,
    MediumBlob,
    LongBlob,
    Blob,
    VarString,
    String,
    Geometry,
}

impl ColumnDataType {
    fn from_row(row: &MySqlRow) -> Self {
        let data_type_str = row.try_get::<String, _>("DATA_TYPE").unwrap_or_default();
        Self::try_from(data_type_str.as_str()).unwrap_or(Self::String)
    }
}

lazy_static! {
    static ref COLUMN_TYPE_MAP: HashMap<&'static str, ColumnDataType> = {
        let mut m = HashMap::new();
        m.insert("BOOLEAN", ColumnDataType::Tiny);
        m.insert("TINYINT UNSIGNED", ColumnDataType::Tiny);
        m.insert("SMALLINT UNSIGNED", ColumnDataType::Short);
        m.insert("INT UNSIGNED", ColumnDataType::Long);
        m.insert("MEDIUMINT UNSIGNED", ColumnDataType::Int24);
        m.insert("BIGINT UNSIGNED", ColumnDataType::LongLong);
        m.insert("TINYINT", ColumnDataType::Tiny);
        m.insert("SMALLINT", ColumnDataType::Short);
        m.insert("INT", ColumnDataType::Long);
        m.insert("MEDIUMINT", ColumnDataType::Int24);
        m.insert("BIGINT", ColumnDataType::LongLong);
        m.insert("FLOAT", ColumnDataType::Float);
        m.insert("DOUBLE", ColumnDataType::Double);
        m.insert("NULL", ColumnDataType::Null);
        m.insert("TIMESTAMP", ColumnDataType::Timestamp);
        m.insert("DATE", ColumnDataType::Date);
        m.insert("TIME", ColumnDataType::Time);
        m.insert("DATETIME", ColumnDataType::Datetime);
        m.insert("YEAR", ColumnDataType::Year);
        m.insert("BIT", ColumnDataType::Bit);
        m.insert("ENUM", ColumnDataType::Enum);
        m.insert("SET", ColumnDataType::Set);
        m.insert("DECIMAL", ColumnDataType::Decimal);
        m.insert("GEOMETRY", ColumnDataType::Geometry);
        m.insert("JSON", ColumnDataType::Json);
        m.insert("BINARY", ColumnDataType::String);
        m.insert("VARBINARY", ColumnDataType::VarString);
        m.insert("CHAR", ColumnDataType::String);
        m.insert("VARCHAR", ColumnDataType::VarChar);
        m.insert("TINYBLOB", ColumnDataType::TinyBlob);
        m.insert("TINYTEXT", ColumnDataType::TinyBlob);
        m.insert("BLOB", ColumnDataType::Blob);
        m.insert("TEXT", ColumnDataType::Blob);
        m.insert("MEDIUMBLOB", ColumnDataType::MediumBlob);
        m.insert("MEDIUMTEXT", ColumnDataType::MediumBlob);
        m.insert("LONGBLOB", ColumnDataType::LongBlob);
        m.insert("LONGTEXT", ColumnDataType::LongBlob);
        m
    };
}

impl TryFrom<&str> for ColumnDataType {
    type Error = String;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        COLUMN_TYPE_MAP
            .get(s.to_uppercase().as_str())
            .copied()
            .ok_or_else(|| format!("Unknown column type: {}", s))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ColumnValue {
    Int(i64),
    Float(f64),
    String(String),
    Boolean(bool),
    Json(serde_json::Value),
    Uuid(Uuid),
    Bytes(Vec<u8>),
    Date(NaiveDate),
    Timestamp(NaiveDateTime),
}

impl ColumnValue {
    pub fn from_row(row: &MySqlRow, data_type: ColumnDataType, name: &str) -> Option<Self> {
        match data_type {
            ColumnDataType::Int24 | ColumnDataType::Long => row
                .try_get::<i32, _>(name)
                .ok()
                .map(|v| Self::Int(v as i64)),
            ColumnDataType::Float => row.try_get::<f64, _>(name).ok().map(Self::Float),
            ColumnDataType::Decimal => row
                .try_get::<BigDecimal, _>(name)
                .ok()
                .and_then(|v| v.to_f64().map(Self::Float)),
            ColumnDataType::String | ColumnDataType::VarChar => {
                row.try_get::<String, _>(name).ok().map(Self::String)
            }
            ColumnDataType::Json => row.try_get::<Value, _>(name).ok().map(Self::Json),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnData {
    pub name: String,
    pub value: Option<ColumnValue>,
    pub type_info: ColumnDataType,
}

impl fmt::Display for ColumnValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ColumnValue::Int(v) => write!(f, "{}", v),
            ColumnValue::Float(v) => write!(f, "{:.15}", v),
            ColumnValue::String(v) => write!(f, "'{}'", v),
            ColumnValue::Boolean(v) => write!(f, "{}", v),
            ColumnValue::Json(v) => write!(f, "{}", v),
            ColumnValue::Uuid(v) => write!(f, "{}", v),
            ColumnValue::Bytes(v) => write!(f, "{:?}", v),
            ColumnValue::Date(v) => write!(f, "{}", v),
            ColumnValue::Timestamp(v) => write!(f, "{}", v),
        }
    }
}
