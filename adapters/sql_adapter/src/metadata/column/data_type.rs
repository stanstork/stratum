use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::row::row::DbRow;

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

impl ColumnDataType {
    pub fn from_row(row: &DbRow) -> Self {
        match row {
            DbRow::MySqlRow(row) => Self::from_sqlx_row(*row),
            DbRow::PostgresRow(row) => Self::from_sqlx_row(*row),
        }
    }

    fn from_sqlx_row<'r, T: sqlx::Row>(row: &'r T) -> Self
    where
        String: sqlx::Decode<'r, T::Database> + sqlx::Type<T::Database>,
        for<'q> &'q str: sqlx::ColumnIndex<T>,
    {
        let data_type_str: String = row.try_get("data_type").unwrap_or_default();
        Self::try_from(data_type_str.as_str()).unwrap_or(Self::String)
    }
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

impl ColumnDataType {
    pub fn to_pg_string(&self) -> String {
        match self {
            ColumnDataType::Decimal => "DECIMAL".to_string(),
            ColumnDataType::Tiny => "SMALLINT".to_string(),
            ColumnDataType::Short => "SMALLINT".to_string(),
            ColumnDataType::Long => "INTEGER".to_string(),
            ColumnDataType::Float => "REAL".to_string(),
            ColumnDataType::Double => "DOUBLE PRECISION".to_string(),
            ColumnDataType::Null => "NULL".to_string(),
            ColumnDataType::Timestamp => "TIMESTAMP".to_string(),
            ColumnDataType::LongLong => "BIGINT".to_string(),
            ColumnDataType::Int24 => "INTEGER".to_string(),
            ColumnDataType::Date => "DATE".to_string(),
            ColumnDataType::Time => "TIME".to_string(),
            ColumnDataType::Datetime => "TIMESTAMP".to_string(),
            ColumnDataType::Year => "INTEGER".to_string(),
            ColumnDataType::VarChar => "VARCHAR".to_string(),
            ColumnDataType::Bit => "BIT".to_string(),
            ColumnDataType::Json => "JSON".to_string(),
            ColumnDataType::NewDecimal => "DECIMAL".to_string(),
            ColumnDataType::Enum => "ENUM".to_string(),
            ColumnDataType::Set => "SET".to_string(),
            ColumnDataType::TinyBlob => "BYTEA".to_string(),
            ColumnDataType::MediumBlob => "BYTEA".to_string(),
            ColumnDataType::LongBlob => "BYTEA".to_string(),
            ColumnDataType::Blob => "BYTEA".to_string(),
            ColumnDataType::VarString => "VARCHAR".to_string(),
            ColumnDataType::String => "TEXT".to_string(),
            ColumnDataType::Geometry => "BYTEA".to_string(),
        }
    }
}
