use super::data_type::ColumnDataType;
use crate::row::db_row::DbRow;
use bigdecimal::ToPrimitive;
use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
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
    pub fn from_row(row: &DbRow, data_type: ColumnDataType, name: &str) -> Option<Self> {
        match data_type {
            ColumnDataType::Int24 | ColumnDataType::Long => {
                row.try_get_i32(name).map(|v| Self::Int(v as i64))
            }
            ColumnDataType::Float => row.try_get_f64(name).map(Self::Float),
            ColumnDataType::Decimal => row
                .try_get_bigdecimal(name)
                .and_then(|v| v.to_f64().map(Self::Float)),
            ColumnDataType::String | ColumnDataType::VarChar => {
                row.try_get_string(name).map(Self::String)
            }
            ColumnDataType::Json => row.try_get_json(name).map(Self::Json),
            ColumnDataType::Timestamp => row.try_get_timestamp(name).map(Self::Timestamp),
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
            ColumnValue::Timestamp(v) => write!(f, "'{}'", v),
        }
    }
}
