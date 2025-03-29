use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum ColumnDataType {
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
    Bytea,
    VarString,
    String,
    Geometry,
    Array,
    Char,
}

lazy_static! {
    static ref COLUMN_TYPE_MAP: HashMap<&'static str, ColumnDataType> = {
        let mut m = HashMap::new();
        m.insert("BOOLEAN", ColumnDataType::Short);
        m.insert("TINYINT UNSIGNED", ColumnDataType::ShortUnsigned);
        m.insert("SMALLINT UNSIGNED", ColumnDataType::ShortUnsigned);
        m.insert("INT UNSIGNED", ColumnDataType::Long);
        m.insert("MEDIUMINT UNSIGNED", ColumnDataType::IntUnsigned);
        m.insert("BIGINT UNSIGNED", ColumnDataType::LongLong);
        m.insert("TINYINT", ColumnDataType::Short);
        m.insert("SMALLINT", ColumnDataType::Short);
        m.insert("INT", ColumnDataType::Int);
        m.insert("MEDIUMINT", ColumnDataType::Int);
        m.insert("BIGINT", ColumnDataType::Long);
        m.insert("FLOAT", ColumnDataType::Float);
        m.insert("DOUBLE", ColumnDataType::Double);
        m.insert("NULL", ColumnDataType::Null);
        m.insert("TIMESTAMP", ColumnDataType::Timestamp);
        m.insert("DATE", ColumnDataType::Date);
        m.insert("TIME", ColumnDataType::Time);
        m.insert("DATETIME", ColumnDataType::Timestamp);
        m.insert("YEAR", ColumnDataType::Year);
        m.insert("BIT", ColumnDataType::Bit);
        m.insert("ENUM", ColumnDataType::Enum);
        m.insert("SET", ColumnDataType::Set);
        m.insert("DECIMAL", ColumnDataType::Decimal);
        m.insert("GEOMETRY", ColumnDataType::Geometry);
        m.insert("JSON", ColumnDataType::Json);
        m.insert("BINARY", ColumnDataType::String);
        m.insert("VARBINARY", ColumnDataType::VarString);
        m.insert("CHAR", ColumnDataType::Char);
        m.insert("VARCHAR", ColumnDataType::VarChar);
        m.insert("TINYBLOB", ColumnDataType::TinyBlob);
        m.insert("TINYTEXT", ColumnDataType::TinyBlob);
        m.insert("BLOB", ColumnDataType::Blob);
        m.insert("TEXT", ColumnDataType::String);
        m.insert("MEDIUMBLOB", ColumnDataType::MediumBlob);
        m.insert("MEDIUMTEXT", ColumnDataType::MediumBlob);
        m.insert("LONGBLOB", ColumnDataType::LongBlob);
        m.insert("LONGTEXT", ColumnDataType::LongBlob);
        m.insert("INTEGER", ColumnDataType::Int);
        m.insert("NUMERIC", ColumnDataType::Decimal);
        m.insert("TIMESTAMP WITH TIME ZONE", ColumnDataType::Timestamp);
        m.insert("TIMESTAMP WITHOUT TIME ZONE", ColumnDataType::Timestamp);
        m.insert("BYTEA", ColumnDataType::Bytea);
        m.insert("ARRAY", ColumnDataType::Array);
        m.insert("CHARACTER", ColumnDataType::Char);
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

impl fmt::Display for ColumnDataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ColumnDataType::Decimal => write!(f, "DECIMAL"),
            ColumnDataType::Short => write!(f, "SMALLINT"),
            ColumnDataType::Long => write!(f, "INT"),
            ColumnDataType::Float => write!(f, "FLOAT"),
            ColumnDataType::Double => write!(f, "DOUBLE"),
            ColumnDataType::Null => write!(f, "NULL"),
            ColumnDataType::Timestamp => write!(f, "TIMESTAMP"),
            ColumnDataType::LongLong => write!(f, "BIGINT"),
            ColumnDataType::Int => write!(f, "INT"),
            ColumnDataType::Time => write!(f, "TIME"),
            ColumnDataType::Year => write!(f, "YEAR"),
            ColumnDataType::VarChar => write!(f, "VARCHAR"),
            ColumnDataType::Bit => write!(f, "BIT"),
            ColumnDataType::Json => write!(f, "JSON"),
            ColumnDataType::NewDecimal => write!(f, "NEWDECIMAL"),
            ColumnDataType::Enum => write!(f, "ENUM"),
            ColumnDataType::Set => write!(f, "SET"),
            ColumnDataType::TinyBlob => write!(f, "TINYBLOB"),
            ColumnDataType::MediumBlob => write!(f, "MEDIUMBLOB"),
            ColumnDataType::LongBlob => write!(f, "LONGBLOB"),
            ColumnDataType::Blob => write!(f, "BLOB"),
            ColumnDataType::VarString => write!(f, "VARSTRING"),
            ColumnDataType::String => write!(f, "STRING"),
            ColumnDataType::Geometry => write!(f, "GEOMETRY"),
            ColumnDataType::Boolean => write!(f, "BOOLEAN"),
            ColumnDataType::ShortUnsigned => write!(f, "SMALLINT UNSIGNED"),
            ColumnDataType::IntUnsigned => write!(f, "INT UNSIGNED"),
            ColumnDataType::Bytea => write!(f, "BYTEA"),
            ColumnDataType::Array => write!(f, "ARRAY"),
            ColumnDataType::Char => write!(f, "CHAR"),
            ColumnDataType::Date => write!(f, "DATE"),
        }
    }
}

impl ColumnDataType {
    // TODO: Handle enum types correctly, for now just treat them as strings
    pub fn is_compatible(&self, other: &ColumnDataType) -> bool {
        match (self, other) {
            (ColumnDataType::Int, ColumnDataType::IntUnsigned)
            | (ColumnDataType::IntUnsigned, ColumnDataType::Int)
            | (ColumnDataType::Short, ColumnDataType::ShortUnsigned)
            | (ColumnDataType::ShortUnsigned, ColumnDataType::Short)
            | (ColumnDataType::Long, ColumnDataType::IntUnsigned)
            | (ColumnDataType::IntUnsigned, ColumnDataType::Long)
            | (ColumnDataType::LongLong, ColumnDataType::Long)
            | (ColumnDataType::Long, ColumnDataType::LongLong) => true,
            (ColumnDataType::String, ColumnDataType::VarChar)
            | (ColumnDataType::VarChar, ColumnDataType::String) => true,
            (ColumnDataType::Geometry, ColumnDataType::Bytea)
            | (ColumnDataType::Bytea, ColumnDataType::Geometry) => true,
            (ColumnDataType::Blob, ColumnDataType::Bytea)
            | (ColumnDataType::Bytea, ColumnDataType::Blob) => true,
            (ColumnDataType::Enum, ColumnDataType::String)
            | (ColumnDataType::String, ColumnDataType::Enum) => true,
            (ColumnDataType::Set, ColumnDataType::Array)
            | (ColumnDataType::Array, ColumnDataType::Set) => true,
            (ColumnDataType::Year, ColumnDataType::Int)
            | (ColumnDataType::Int, ColumnDataType::Year) => true,
            (ColumnDataType::MediumBlob, ColumnDataType::Bytea)
            | (ColumnDataType::Bytea, ColumnDataType::MediumBlob) => true,
            (ColumnDataType::Date, ColumnDataType::Timestamp)
            | (ColumnDataType::Timestamp, ColumnDataType::Date) => true,
            _ => self == other,
        }
    }
}
