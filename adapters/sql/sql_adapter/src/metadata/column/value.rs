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
            ColumnDataType::Int | ColumnDataType::Long | ColumnDataType::Short => {
                row.try_get_i64(name).map(Self::Int)
            }
            ColumnDataType::IntUnsigned | ColumnDataType::ShortUnsigned | ColumnDataType::Year => {
                row.try_get_u64(name).map(|v| Self::Int(v as i64))
            }
            ColumnDataType::Float => row.try_get_f64(name).map(Self::Float),
            ColumnDataType::Decimal => row
                .try_get_bigdecimal(name)
                .and_then(|v| v.to_f64().map(Self::Float)),
            ColumnDataType::String | ColumnDataType::VarChar | ColumnDataType::Char => {
                row.try_get_string(name).map(Self::String)
            }
            ColumnDataType::Boolean => row.try_get_bool(name).map(Self::Boolean),
            ColumnDataType::Json => row.try_get_json(name).map(Self::Json),
            ColumnDataType::Timestamp => row.try_get_timestamp(name).map(Self::Timestamp),
            ColumnDataType::Date => row.try_get_date(name).map(Self::Date),
            ColumnDataType::Enum => row.try_get_string(name).map(Self::String),
            ColumnDataType::Bytea => row.try_get_bytes(name).map(Self::Bytes),
            ColumnDataType::Blob
            | ColumnDataType::TinyBlob
            | ColumnDataType::MediumBlob
            | ColumnDataType::LongBlob => row.try_get_bytes(name).map(Self::Bytes),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            ColumnValue::Int(v) => Some(*v as f64),
            ColumnValue::Float(v) => Some(*v),
            ColumnValue::String(v) => v.parse::<f64>().ok(),
            ColumnValue::Boolean(v) => Some(if *v { 1.0 } else { 0.0 }),
            ColumnValue::Json(v) => v.as_f64(),
            ColumnValue::Uuid(_) => None,
            ColumnValue::Bytes(_) => None,
            ColumnValue::Date(_) => None,
            ColumnValue::Timestamp(_) => None,
        }
    }

    pub fn as_string(&self) -> Option<String> {
        match self {
            ColumnValue::Int(v) => Some(v.to_string()),
            ColumnValue::Float(v) => Some(v.to_string()),
            ColumnValue::String(v) => Some(v.clone()),
            ColumnValue::Boolean(v) => Some(v.to_string()),
            ColumnValue::Json(v) => v.as_str().map(|s| s.to_string()),
            ColumnValue::Uuid(v) => Some(v.to_string()),
            ColumnValue::Bytes(_) => None,
            ColumnValue::Date(_) => None,
            ColumnValue::Timestamp(_) => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnData {
    pub name: String,
    pub value: Option<ColumnValue>,
}

impl fmt::Display for ColumnValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ColumnValue::Int(v) => write!(f, "{}", v),
            ColumnValue::Float(v) => write!(f, "{:.15}", v),
            ColumnValue::String(v) => write!(f, "'{}'", v.replace("'", "''")),
            ColumnValue::Boolean(v) => write!(f, "{}", v),
            ColumnValue::Json(v) => {
                let json_str = v.to_string().replace('\'', "''");
                write!(f, "'{}'", json_str)
            }
            ColumnValue::Uuid(v) => write!(f, "{}", v),
            ColumnValue::Bytes(v) => {
                let hex = v
                    .iter()
                    .fold(String::new(), |acc, byte| acc + &format!("{:02x}", byte));
                write!(f, "E'\\\\x{}'", hex)
            }
            ColumnValue::Date(v) => write!(f, "'{}'", v),
            ColumnValue::Timestamp(v) => write!(f, "'{}'", v),
        }
    }
}
