use super::data_type::ColumnDataType;
use bigdecimal::{BigDecimal, ToPrimitive};
use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::mysql::MySqlRow;
use sqlx::postgres::PgRow;
use sqlx::Row;
use std::fmt;
use uuid::Uuid;

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
    Timestamp(DateTime<Utc>),
}

impl ColumnValue {
    pub fn from_mysql_row(row: &MySqlRow, data_type: ColumnDataType, name: &str) -> Option<Self> {
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
            ColumnDataType::Timestamp => row
                .try_get::<DateTime<Utc>, _>(name)
                .ok()
                .map(Self::Timestamp),
            _ => None,
        }
    }

    pub fn from_pg_row(row: &PgRow, data_type: ColumnDataType, name: &str) -> Option<Self> {
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
            ColumnDataType::Timestamp => row
                .try_get::<DateTime<Utc>, _>(name)
                .ok()
                .map(Self::Timestamp),
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
