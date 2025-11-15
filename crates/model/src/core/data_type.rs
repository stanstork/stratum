use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::{borrow::Cow, collections::HashMap, fmt};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DataType {
    Decimal,
    Short,
    ShortUnsigned,
    Long,
    Float,
    Double,
    Boolean,
    Null,
    Date,
    Timestamp,
    LongLong,
    Int,
    Int4,
    IntUnsigned,
    Time,
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
    Binary,
    VarBinary,
    Bytea,
    String,
    Geometry,
    Array(Option<String>),
    Char,
    Custom(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlDialect {
    MySql,
    Postgres,
}

lazy_static! {
    static ref MYSQL_TYPE_MAP: HashMap<&'static str, DataType> = build_mysql_type_map();
    static ref POSTGRES_TYPE_MAP: HashMap<&'static str, DataType> = build_postgres_type_map();
}

impl DataType {
    pub fn from_mysql_type(type_name: &str) -> Result<Self, String> {
        let normalized = Self::normalize_type_name(type_name);
        MYSQL_TYPE_MAP
            .get(normalized.as_str())
            .cloned()
            .ok_or_else(|| format!("Unknown MySQL column type: {type_name}"))
    }

    pub fn from_postgres_type(type_name: &str) -> Result<Self, String> {
        if let Some(array_name) = Self::normalize_postgres_array_type(type_name) {
            return Ok(DataType::Array(Some(array_name)));
        }

        let normalized = Self::normalize_type_name(type_name);
        POSTGRES_TYPE_MAP
            .get(normalized.as_str())
            .cloned()
            .ok_or_else(|| format!("Unknown Postgres column type: {type_name}"))
    }

    pub fn mysql_name(&self) -> Cow<'_, str> {
        match self {
            DataType::Decimal | DataType::NewDecimal => Cow::Borrowed("DECIMAL"),
            DataType::Short => Cow::Borrowed("SMALLINT"),
            DataType::ShortUnsigned => Cow::Borrowed("SMALLINT UNSIGNED"),
            DataType::Long => Cow::Borrowed("BIGINT"),
            DataType::LongLong => Cow::Borrowed("BIGINT UNSIGNED"),
            DataType::Int => Cow::Borrowed("INT"),
            DataType::Int4 => Cow::Borrowed("INT"),
            DataType::IntUnsigned => Cow::Borrowed("INT UNSIGNED"),
            DataType::Float => Cow::Borrowed("FLOAT"),
            DataType::Double => Cow::Borrowed("DOUBLE"),
            DataType::Boolean => Cow::Borrowed("BOOLEAN"),
            DataType::Null => Cow::Borrowed("NULL"),
            DataType::Timestamp => Cow::Borrowed("TIMESTAMP"),
            DataType::Date => Cow::Borrowed("DATE"),
            DataType::Time => Cow::Borrowed("TIME"),
            DataType::Year => Cow::Borrowed("YEAR"),
            DataType::VarChar => Cow::Borrowed("VARCHAR"),
            DataType::Char => Cow::Borrowed("CHAR"),
            DataType::String => Cow::Borrowed("TEXT"),
            DataType::Bit => Cow::Borrowed("BIT"),
            DataType::Json => Cow::Borrowed("JSON"),
            DataType::Enum => Cow::Borrowed("ENUM"),
            DataType::Set => Cow::Borrowed("SET"),
            DataType::TinyBlob => Cow::Borrowed("TINYBLOB"),
            DataType::MediumBlob => Cow::Borrowed("MEDIUMBLOB"),
            DataType::LongBlob => Cow::Borrowed("LONGBLOB"),
            DataType::Blob => Cow::Borrowed("BLOB"),
            DataType::Binary => Cow::Borrowed("BINARY"),
            DataType::VarBinary => Cow::Borrowed("VARBINARY"),
            DataType::Bytea => Cow::Borrowed("BLOB"),
            DataType::Geometry => Cow::Borrowed("GEOMETRY"),
            DataType::Array(array_name) => match array_name {
                Some(name) => Cow::Owned(name.clone()),
                None => Cow::Borrowed("ARRAY"),
            },
            DataType::Custom(name) => Cow::Borrowed(name),
        }
    }

    pub fn postgres_name(&self) -> Cow<'_, str> {
        match self {
            DataType::Decimal | DataType::NewDecimal => Cow::Borrowed("DECIMAL"),
            DataType::Short | DataType::ShortUnsigned => Cow::Borrowed("SMALLINT"),
            DataType::Long | DataType::LongLong => Cow::Borrowed("BIGINT"),
            DataType::Int | DataType::Int4 | DataType::IntUnsigned => Cow::Borrowed("INTEGER"),
            DataType::Float => Cow::Borrowed("REAL"),
            DataType::Double => Cow::Borrowed("DOUBLE PRECISION"),
            DataType::Boolean => Cow::Borrowed("BOOLEAN"),
            DataType::Null => Cow::Borrowed("NULL"),
            DataType::Timestamp => Cow::Borrowed("TIMESTAMP"),
            DataType::Date => Cow::Borrowed("DATE"),
            DataType::Time => Cow::Borrowed("TIME"),
            DataType::Year => Cow::Borrowed("INTEGER"),
            DataType::VarChar => Cow::Borrowed("VARCHAR"),
            DataType::Char => Cow::Borrowed("CHAR"),
            DataType::String => Cow::Borrowed("TEXT"),
            DataType::Bit => Cow::Borrowed("BIT"),
            DataType::Json => Cow::Borrowed("JSONB"),
            DataType::Enum => Cow::Borrowed("ENUM"),
            DataType::Set => Cow::Borrowed("TEXT[]"),
            DataType::Array(Some(name)) => Cow::Owned(name.clone()),
            DataType::Array(None) => Cow::Borrowed("TEXT[]"),
            DataType::TinyBlob
            | DataType::MediumBlob
            | DataType::LongBlob
            | DataType::Blob
            | DataType::Binary
            | DataType::VarBinary
            | DataType::Bytea
            | DataType::Geometry => Cow::Borrowed("BYTEA"),
            DataType::Custom(name) => Cow::Borrowed(name),
        }
    }

    pub fn is_compatible(&self, other: &DataType) -> bool {
        match (self, other) {
            (DataType::Int, DataType::IntUnsigned)
            | (DataType::IntUnsigned, DataType::Int)
            | (DataType::Short, DataType::ShortUnsigned)
            | (DataType::ShortUnsigned, DataType::Short)
            | (DataType::Long, DataType::IntUnsigned)
            | (DataType::IntUnsigned, DataType::Long)
            | (DataType::LongLong, DataType::Long)
            | (DataType::Long, DataType::LongLong) => true,
            (DataType::Int4, DataType::Int) | (DataType::Int, DataType::Int4) => true,
            (DataType::Int, DataType::Short) | (DataType::Short, DataType::Int) => true,
            (DataType::String, DataType::VarChar) | (DataType::VarChar, DataType::String) => true,
            (DataType::Geometry, DataType::Bytea) | (DataType::Bytea, DataType::Geometry) => true,
            (DataType::Geometry, DataType::Binary) | (DataType::Binary, DataType::Geometry) => true,
            (DataType::Geometry, DataType::VarBinary)
            | (DataType::VarBinary, DataType::Geometry) => true,
            (DataType::Blob, DataType::Bytea) | (DataType::Bytea, DataType::Blob) => true,
            (DataType::TinyBlob, DataType::Bytea) | (DataType::Bytea, DataType::TinyBlob) => true,
            (DataType::MediumBlob, DataType::Bytea) | (DataType::Bytea, DataType::MediumBlob) => {
                true
            }
            (DataType::LongBlob, DataType::Bytea) | (DataType::Bytea, DataType::LongBlob) => true,
            (DataType::Binary, DataType::Bytea) | (DataType::Bytea, DataType::Binary) => true,
            (DataType::VarBinary, DataType::Bytea) | (DataType::Bytea, DataType::VarBinary) => true,
            (DataType::Enum, DataType::String) | (DataType::String, DataType::Enum) => true,
            (DataType::Set, DataType::Array(_)) | (DataType::Array(_), DataType::Set) => true,
            (DataType::Year, DataType::Int) | (DataType::Int, DataType::Year) => true,
            (DataType::Date, DataType::Timestamp) | (DataType::Timestamp, DataType::Date) => true,
            _ => self == other,
        }
    }

    pub fn supports_length(&self, dialect: SqlDialect) -> bool {
        match dialect {
            SqlDialect::Postgres => matches!(self, DataType::VarChar | DataType::Char),
            SqlDialect::MySql => {
                matches!(
                    self,
                    DataType::VarChar | DataType::Char | DataType::Binary | DataType::VarBinary
                )
            }
        }
    }

    fn from_known_type(type_name: &str) -> Option<Self> {
        if let Some(array_name) = Self::normalize_postgres_array_type(type_name) {
            return Some(DataType::Array(Some(array_name)));
        }

        let normalized = Self::normalize_type_name(type_name);
        MYSQL_TYPE_MAP
            .get(normalized.as_str())
            .or_else(|| POSTGRES_TYPE_MAP.get(normalized.as_str()))
            .cloned()
    }

    fn normalize_type_name(type_name: &str) -> String {
        type_name.trim().to_uppercase()
    }

    fn normalize_postgres_array_type(type_name: &str) -> Option<String> {
        let trimmed = type_name.trim();
        if trimmed.starts_with('_') {
            let base = trimmed.trim_start_matches('_').trim();
            if base.is_empty() {
                None
            } else {
                Some(format!("{}[]", Self::normalize_type_name(base)))
            }
        } else if trimmed.ends_with("[]") {
            let base = trimmed.trim_end_matches("[]").trim();
            if base.is_empty() {
                None
            } else {
                Some(format!("{}[]", Self::normalize_type_name(base)))
            }
        } else {
            None
        }
    }
}

impl TryFrom<&str> for DataType {
    type Error = String;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        DataType::from_known_type(s).ok_or_else(|| format!("Unknown column type: {s}"))
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.mysql_name())
    }
}

fn build_mysql_type_map() -> HashMap<&'static str, DataType> {
    use DataType::*;

    let entries = [
        ("BOOLEAN", Boolean),
        ("BOOL", Boolean),
        ("TINYINT", Short),
        ("SMALLINT", Short),
        ("TINYINT UNSIGNED", ShortUnsigned),
        ("SMALLINT UNSIGNED", ShortUnsigned),
        ("MEDIUMINT", Int),
        ("MEDIUMINT UNSIGNED", IntUnsigned),
        ("INT", Int),
        ("INTEGER", Int),
        ("INT UNSIGNED", Long),
        ("INTEGER UNSIGNED", Long),
        ("BIGINT", Long),
        ("BIGINT UNSIGNED", LongLong),
        ("FLOAT", Float),
        ("DOUBLE", Double),
        ("DOUBLE PRECISION", Double),
        ("DECIMAL", Decimal),
        ("NUMERIC", Decimal),
        ("NEWDECIMAL", NewDecimal),
        ("NULL", Null),
        ("TIMESTAMP", Timestamp),
        ("DATETIME", Timestamp),
        ("DATE", Date),
        ("TIME", Time),
        ("YEAR", Year),
        ("BIT", Bit),
        ("ENUM", Enum),
        ("SET", Set),
        ("JSON", Json),
        ("GEOMETRY", Geometry),
        ("CHAR", Char),
        ("CHARACTER", Char),
        ("VARCHAR", VarChar),
        ("CHARACTER VARYING", VarChar),
        ("TEXT", String),
        ("TINYTEXT", String),
        ("MEDIUMTEXT", String),
        ("LONGTEXT", String),
        ("BINARY", Binary),
        ("VARBINARY", VarBinary),
        ("TINYBLOB", TinyBlob),
        ("BLOB", Blob),
        ("MEDIUMBLOB", MediumBlob),
        ("LONGBLOB", LongBlob),
        ("ARRAY", Array(None)),
    ];

    let mut map = HashMap::new();
    for (name, data_type) in entries {
        map.insert(name, data_type);
    }
    map
}

fn build_postgres_type_map() -> HashMap<&'static str, DataType> {
    use DataType::*;

    let entries = [
        ("BOOLEAN", Boolean),
        ("BOOL", Boolean),
        ("SMALLINT", Short),
        ("INT2", Short),
        ("INTEGER", Int),
        ("INT", Int),
        ("INT4", Int4),
        ("INT8", Long),
        ("BIGINT", Long),
        ("FLOAT4", Float),
        ("REAL", Float),
        ("FLOAT8", Double),
        ("DOUBLE PRECISION", Double),
        ("NUMERIC", Decimal),
        ("DECIMAL", Decimal),
        ("JSONB", Json),
        ("JSON", Json),
        ("TEXT", String),
        ("NAME", String),
        ("XML", String),
        ("CHARACTER VARYING", VarChar),
        ("VARCHAR", VarChar),
        ("CHARACTER", Char),
        ("CHAR", Char),
        ("BPCHAR", Char),
        ("BYTEA", Bytea),
        ("BIT", Bit),
        ("DATE", Date),
        ("TIME", Time),
        ("TIME WITHOUT TIME ZONE", Time),
        ("TIME WITH TIME ZONE", Time),
        ("TIMETZ", Time),
        ("TIMESTAMP", Timestamp),
        ("TIMESTAMP WITHOUT TIME ZONE", Timestamp),
        ("TIMESTAMP WITH TIME ZONE", Timestamp),
        ("TIMESTAMPTZ", Timestamp),
        ("GEOMETRY", Geometry),
        ("ARRAY", Array(None)),
    ];

    let mut map = HashMap::new();
    for (name, data_type) in entries {
        map.insert(name, data_type);
    }
    map
}
