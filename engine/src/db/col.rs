use serde::{Deserialize, Serialize};
use sqlx::mysql::MySqlColumn;
use sqlx::{Column, TypeInfo};

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

impl From<&str> for ColumnType {
    fn from(s: &str) -> Self {
        match s.to_uppercase().as_str() {
            "BOOLEAN" => Self::Tiny,
            "TINYINT UNSIGNED" => Self::Tiny,
            "SMALLINT UNSIGNED" => Self::Short,
            "INT UNSIGNED" => Self::Long,
            "MEDIUMINT UNSIGNED" => Self::Int24,
            "BIGINT UNSIGNED" => Self::LongLong,
            "TINYINT" => Self::Tiny,
            "SMALLINT" => Self::Short,
            "INT" => Self::Long,
            "MEDIUMINT" => Self::Int24,
            "BIGINT" => Self::LongLong,
            "FLOAT" => Self::Float,
            "DOUBLE" => Self::Double,
            "NULL" => Self::Null,
            "TIMESTAMP" => Self::Timestamp,
            "DATE" => Self::Date,
            "TIME" => Self::Time,
            "DATETIME" => Self::Datetime,
            "YEAR" => Self::Year,
            "BIT" => Self::Bit,
            "ENUM" => Self::Enum,
            "SET" => Self::Set,
            "DECIMAL" => Self::Decimal,
            "GEOMETRY" => Self::Geometry,
            "JSON" => Self::Json,
            "BINARY" => Self::String,
            "VARBINARY" => Self::VarString,
            "CHAR" => Self::String,
            "VARCHAR" => Self::VarChar,
            "TINYBLOB" => Self::TinyBlob,
            "TINYTEXT" => Self::TinyBlob,
            "BLOB" => Self::Blob,
            "TEXT" => Self::Blob,
            "MEDIUMBLOB" => Self::MediumBlob,
            "MEDIUMTEXT" => Self::MediumBlob,
            "LONGBLOB" => Self::LongBlob,
            "LONGTEXT" => Self::LongBlob,
            _ => panic!("Unknown column type: {}", s),
        }
    }
}

pub struct ColumnFlags {
    flags: u16,
}

impl ColumnFlags {
    pub fn from_bits_truncate(bits: u16) -> Self {
        Self { flags: bits }
    }

    pub fn contains(&self, flags: ColumnFlags) -> bool {
        self.flags & flags.flags != 0
    }

    pub fn is_empty(&self) -> bool {
        self.flags == 0
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ColumnMetadata {
    pub(crate) ordinal: usize,
    pub(crate) name: String,
    pub(crate) type_info: ColumnType,
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
