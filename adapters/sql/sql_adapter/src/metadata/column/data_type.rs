use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
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
        m.insert("ENUM", ColumnDataType::Enum); // TODO: Enum support
        m.insert("SET", ColumnDataType::Set); // TODO: Set support
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
