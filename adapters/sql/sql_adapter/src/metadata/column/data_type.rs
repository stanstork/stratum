use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum ColumnDataType {
    Decimal,
    Short,
    Long,
    Float,
    Double,
    Boolean,
    Null,
    Timestamp,
    LongLong,
    Int24,
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
    VarString,
    String,
    Geometry,
}

lazy_static! {
    static ref COLUMN_TYPE_MAP: HashMap<&'static str, ColumnDataType> = {
        let mut m = HashMap::new();
        m.insert("BOOLEAN", ColumnDataType::Boolean);
        m.insert("TINYINT UNSIGNED", ColumnDataType::Boolean);
        m.insert("SMALLINT UNSIGNED", ColumnDataType::Short);
        m.insert("INT UNSIGNED", ColumnDataType::Long);
        m.insert("MEDIUMINT UNSIGNED", ColumnDataType::Int24);
        m.insert("BIGINT UNSIGNED", ColumnDataType::LongLong);
        m.insert("TINYINT", ColumnDataType::Short);
        m.insert("SMALLINT", ColumnDataType::Short);
        m.insert("INT", ColumnDataType::Long);
        m.insert("MEDIUMINT", ColumnDataType::Int24);
        m.insert("BIGINT", ColumnDataType::LongLong);
        m.insert("FLOAT", ColumnDataType::Float);
        m.insert("DOUBLE", ColumnDataType::Double);
        m.insert("NULL", ColumnDataType::Null);
        m.insert("TIMESTAMP", ColumnDataType::Timestamp);
        m.insert("DATE", ColumnDataType::Timestamp);
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
        m.insert("INTEGER", ColumnDataType::Long);
        m.insert("NUMERIC", ColumnDataType::Decimal);
        m.insert("TIMESTAMP WITH TIME ZONE", ColumnDataType::Timestamp);
        m.insert("TIMESTAMP WITHOUT TIME ZONE", ColumnDataType::Timestamp);
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
            ColumnDataType::Int24 => write!(f, "MEDIUMINT"),
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
        }
    }
}
