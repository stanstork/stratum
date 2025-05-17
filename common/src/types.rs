use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
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
    static ref TYPE_MAP: HashMap<&'static str, DataType> = {
        let mut m = HashMap::new();
        m.insert("BOOLEAN", DataType::Short);
        m.insert("TINYINT UNSIGNED", DataType::ShortUnsigned);
        m.insert("SMALLINT UNSIGNED", DataType::ShortUnsigned);
        m.insert("INT UNSIGNED", DataType::Long);
        m.insert("MEDIUMINT UNSIGNED", DataType::IntUnsigned);
        m.insert("BIGINT UNSIGNED", DataType::LongLong);
        m.insert("TINYINT", DataType::Short);
        m.insert("SMALLINT", DataType::Short);
        m.insert("INT", DataType::Int);
        m.insert("INT8", DataType::Int);
        m.insert("MEDIUMINT", DataType::Int);
        m.insert("BIGINT", DataType::Long);
        m.insert("FLOAT", DataType::Float);
        m.insert("DOUBLE", DataType::Double);
        m.insert("NULL", DataType::Null);
        m.insert("TIMESTAMP", DataType::Timestamp);
        m.insert("DATE", DataType::Date);
        m.insert("TIME", DataType::Time);
        m.insert("DATETIME", DataType::Timestamp);
        m.insert("YEAR", DataType::Year);
        m.insert("BIT", DataType::Bit);
        m.insert("ENUM", DataType::Enum);
        m.insert("SET", DataType::Set);
        m.insert("DECIMAL", DataType::Decimal);
        m.insert("GEOMETRY", DataType::Geometry);
        m.insert("JSON", DataType::Json);
        m.insert("BINARY", DataType::String);
        m.insert("VARBINARY", DataType::VarString);
        m.insert("CHAR", DataType::Char);
        m.insert("VARCHAR", DataType::VarChar);
        m.insert("TINYBLOB", DataType::TinyBlob);
        m.insert("TINYTEXT", DataType::TinyBlob);
        m.insert("BLOB", DataType::Blob);
        m.insert("TEXT", DataType::String);
        m.insert("MEDIUMBLOB", DataType::MediumBlob);
        m.insert("MEDIUMTEXT", DataType::MediumBlob);
        m.insert("LONGBLOB", DataType::LongBlob);
        m.insert("LONGTEXT", DataType::LongBlob);
        m.insert("INTEGER", DataType::Int);
        m.insert("NUMERIC", DataType::Decimal);
        m.insert("TIMESTAMP WITH TIME ZONE", DataType::Timestamp);
        m.insert("TIMESTAMP WITHOUT TIME ZONE", DataType::Timestamp);
        m.insert("BYTEA", DataType::Bytea);
        m.insert("ARRAY", DataType::Array);
        m.insert("CHARACTER", DataType::Char);
        m
    };
}

impl TryFrom<&str> for DataType {
    type Error = String;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        TYPE_MAP
            .get(s.to_uppercase().as_str())
            .copied()
            .ok_or_else(|| format!("Unknown column type: {}", s))
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::Decimal => write!(f, "DECIMAL"),
            DataType::Short => write!(f, "SMALLINT"),
            DataType::Long => write!(f, "INT"),
            DataType::Float => write!(f, "FLOAT"),
            DataType::Double => write!(f, "DOUBLE"),
            DataType::Null => write!(f, "NULL"),
            DataType::Timestamp => write!(f, "TIMESTAMP"),
            DataType::LongLong => write!(f, "BIGINT"),
            DataType::Int => write!(f, "INT"),
            DataType::Time => write!(f, "TIME"),
            DataType::Year => write!(f, "YEAR"),
            DataType::VarChar => write!(f, "VARCHAR"),
            DataType::Bit => write!(f, "BIT"),
            DataType::Json => write!(f, "JSON"),
            DataType::NewDecimal => write!(f, "NEWDECIMAL"),
            DataType::Enum => write!(f, "ENUM"),
            DataType::Set => write!(f, "SET"),
            DataType::TinyBlob => write!(f, "TINYBLOB"),
            DataType::MediumBlob => write!(f, "MEDIUMBLOB"),
            DataType::LongBlob => write!(f, "LONGBLOB"),
            DataType::Blob => write!(f, "BLOB"),
            DataType::VarString => write!(f, "VARSTRING"),
            DataType::String => write!(f, "STRING"),
            DataType::Geometry => write!(f, "GEOMETRY"),
            DataType::Boolean => write!(f, "BOOLEAN"),
            DataType::ShortUnsigned => write!(f, "SMALLINT UNSIGNED"),
            DataType::IntUnsigned => write!(f, "INT UNSIGNED"),
            DataType::Bytea => write!(f, "BYTEA"),
            DataType::Array => write!(f, "ARRAY"),
            DataType::Char => write!(f, "CHAR"),
            DataType::Date => write!(f, "DATE"),
        }
    }
}

impl DataType {
    // TODO: Handle enum types correctly, for now just treat them as strings
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
            (DataType::String, DataType::VarChar) | (DataType::VarChar, DataType::String) => true,
            (DataType::Geometry, DataType::Bytea) | (DataType::Bytea, DataType::Geometry) => true,
            (DataType::Blob, DataType::Bytea) | (DataType::Bytea, DataType::Blob) => true,
            (DataType::Enum, DataType::String) | (DataType::String, DataType::Enum) => true,
            (DataType::Set, DataType::Array) | (DataType::Array, DataType::Set) => true,
            (DataType::Year, DataType::Int) | (DataType::Int, DataType::Year) => true,
            (DataType::MediumBlob, DataType::Bytea) | (DataType::Bytea, DataType::MediumBlob) => {
                true
            }
            (DataType::Date, DataType::Timestamp) | (DataType::Timestamp, DataType::Date) => true,
            _ => self == other,
        }
    }
}
