use chrono::{NaiveDate, NaiveDateTime};
use serde::{Deserialize, Serialize};
use sqlx::mysql::MySqlColumn;
use sqlx::{Column, TypeInfo};
use std::convert::TryFrom;
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
pub struct ColumnMetadata {
    pub ordinal: usize,
    pub name: String,
    pub type_info: ColumnType,
}

impl From<&MySqlColumn> for ColumnMetadata {
    fn from(column: &MySqlColumn) -> Self {
        let type_info = ColumnType::try_from(column.type_info().name()).unwrap_or_else(|err| {
            eprintln!("{}", err);
            ColumnType::String
        });

        Self {
            ordinal: column.ordinal(),
            name: column.name().to_string(),
            type_info,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ColumnType {
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

impl TryFrom<&str> for ColumnType {
    type Error = String;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s.to_uppercase().as_str() {
            "BOOLEAN" => Ok(Self::Tiny),
            "TINYINT UNSIGNED" => Ok(Self::Tiny),
            "SMALLINT UNSIGNED" => Ok(Self::Short),
            "INT UNSIGNED" => Ok(Self::Long),
            "MEDIUMINT UNSIGNED" => Ok(Self::Int24),
            "BIGINT UNSIGNED" => Ok(Self::LongLong),
            "TINYINT" => Ok(Self::Tiny),
            "SMALLINT" => Ok(Self::Short),
            "INT" => Ok(Self::Long),
            "MEDIUMINT" => Ok(Self::Int24),
            "BIGINT" => Ok(Self::LongLong),
            "FLOAT" => Ok(Self::Float),
            "DOUBLE" => Ok(Self::Double),
            "NULL" => Ok(Self::Null),
            "TIMESTAMP" => Ok(Self::Timestamp),
            "DATE" => Ok(Self::Date),
            "TIME" => Ok(Self::Time),
            "DATETIME" => Ok(Self::Datetime),
            "YEAR" => Ok(Self::Year),
            "BIT" => Ok(Self::Bit),
            "ENUM" => Ok(Self::Enum),
            "SET" => Ok(Self::Set),
            "DECIMAL" => Ok(Self::Decimal),
            "GEOMETRY" => Ok(Self::Geometry),
            "JSON" => Ok(Self::Json),
            "BINARY" => Ok(Self::String),
            "VARBINARY" => Ok(Self::VarString),
            "CHAR" => Ok(Self::String),
            "VARCHAR" => Ok(Self::VarChar),
            "TINYBLOB" => Ok(Self::TinyBlob),
            "TINYTEXT" => Ok(Self::TinyBlob),
            "BLOB" => Ok(Self::Blob),
            "TEXT" => Ok(Self::Blob),
            "MEDIUMBLOB" => Ok(Self::MediumBlob),
            "MEDIUMTEXT" => Ok(Self::MediumBlob),
            "LONGBLOB" => Ok(Self::LongBlob),
            "LONGTEXT" => Ok(Self::LongBlob),
            _ => Err(format!("Unknown column type: {}", s)),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
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

#[derive(Debug, Serialize, Deserialize)]
pub struct ColumnData {
    pub name: String,
    pub value: Option<ColumnValue>,
    pub type_info: ColumnType,
}
