use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, fmt, hash::Hash};
use uuid::Uuid;

use crate::types::DataType;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Value {
    Int(i64),
    Float(f64),
    String(String),
    Boolean(bool),
    Json(serde_json::Value),
    Uuid(Uuid),
    Bytes(Vec<u8>),
    Date(NaiveDate),
    Timestamp(DateTime<Utc>),
    Enum(String, String),
    StringArray(Vec<String>),
    Null,
}

impl Eq for Value {}

impl Hash for Value {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        use Value::*;
        std::mem::discriminant(self).hash(state);
        match self {
            Int(v) => v.hash(state),
            Float(v) => {
                // Hash the bits of the float to handle NaN and -0.0 correctly
                let bits = v.to_bits();
                bits.hash(state);
            }
            String(v) => v.hash(state),
            Boolean(v) => v.hash(state),
            Json(v) => {
                // Serialize JSON to a string for hashing
                let json_str = serde_json::to_string(v).unwrap_or_default();
                json_str.hash(state);
            }
            Uuid(v) => v.hash(state),
            Bytes(v) => v.hash(state),
            Date(v) => v.hash(state),
            Timestamp(v) => v.hash(state),
            Enum(name, value) => {
                name.hash(state);
                value.hash(state);
            }
            StringArray(v) => v.hash(state),
            Null => {} // Nothing to hash for Null
        }
    }
}

impl Value {
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Int(v) => Some(*v as f64),
            Value::Float(v) => Some(*v),
            Value::String(v) => v.parse::<f64>().ok(),
            Value::Boolean(v) => Some(if *v { 1.0 } else { 0.0 }),
            Value::Json(v) => v.as_f64(),
            Value::Uuid(_) => None,
            Value::Bytes(_) => None,
            Value::Date(_) => None,
            Value::Timestamp(_) => None,
            Value::Null => None,
            Value::Enum(_, _) => None,
            Value::StringArray(_) => None,
        }
    }

    pub fn as_usize(&self) -> Option<usize> {
        match self {
            Value::Int(v) => Some(*v as usize),
            Value::Float(v) => Some(*v as usize),
            Value::String(v) => v.parse::<usize>().ok(),
            Value::Boolean(v) => Some(if *v { 1 } else { 0 }),
            Value::Json(v) => v.as_u64().map(|u| u as usize),
            Value::Uuid(_) => None,
            Value::Bytes(_) => None,
            Value::Date(_) => None,
            Value::Timestamp(_) => None,
            Value::Null => None,
            Value::Enum(_, _) => None,
            Value::StringArray(_) => None,
        }
    }

    pub fn as_string(&self) -> Option<String> {
        match self {
            Value::Int(v) => Some(v.to_string()),
            Value::Float(v) => Some(v.to_string()),
            Value::String(v) => Some(v.clone()),
            Value::Boolean(v) => Some(v.to_string()),
            Value::Json(v) => v.as_str().map(|s| s.to_string()),
            Value::Uuid(v) => Some(v.to_string()),
            Value::Bytes(_) => None,
            Value::Date(_) => None,
            Value::Timestamp(_) => None,
            Value::Null => Some("NULL".to_string()),
            Value::Enum(_, v) => Some(v.clone()),
            Value::StringArray(v) => Some(format!("{v:?}")),
        }
    }

    pub fn compare(&self, other: &Value) -> Option<Ordering> {
        use Value::*;
        match (self, other) {
            (Int(a), Int(b)) => Some(a.cmp(b)),
            (Float(a), Float(b)) => a.partial_cmp(b),
            (Int(a), Float(b)) => (*a as f64).partial_cmp(b),
            (Float(a), Int(b)) => a.partial_cmp(&(*b as f64)),
            (String(a), String(b)) => Some(a.cmp(b)),
            (Boolean(a), Boolean(b)) => Some(a.cmp(b)),
            (Date(a), Date(b)) => Some(a.cmp(b)),
            (Timestamp(a), Timestamp(b)) => Some(a.cmp(b)),
            _ => None,
        }
    }

    pub fn equal(&self, other: &Value) -> bool {
        self.compare(other) == Some(Ordering::Equal)
    }

    pub fn data_type(&self) -> DataType {
        match self {
            Value::Int(_) => DataType::Int,
            Value::Float(_) => DataType::Float,
            Value::String(_) => DataType::String,
            Value::Boolean(_) => DataType::Boolean,
            Value::Json(_) => DataType::Json,
            Value::Uuid(_) => DataType::VarChar, // UUIDs are typically stored as strings
            Value::Bytes(_) => DataType::Bytea,
            Value::Date(_) => DataType::Date,
            Value::Timestamp(_) => DataType::Timestamp,
            Value::Enum(_, _) => DataType::Enum,
            Value::StringArray(_) => DataType::Array,
            Value::Null => DataType::Null,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldValue {
    pub name: String,
    pub value: Option<Value>,
    pub data_type: DataType,
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Int(v) => write!(f, "{v}"),
            Value::Float(v) => write!(f, "{v:.15}"),
            Value::String(v) => write!(f, "'{}'", v.replace("'", "''")),
            Value::Boolean(v) => write!(f, "{v}"),
            Value::Json(v) => {
                let json_str = v.to_string().replace('\'', "''");
                write!(f, "'{json_str}'")
            }
            Value::Uuid(v) => write!(f, "{v}"),
            Value::Bytes(v) => {
                let hex = v
                    .iter()
                    .fold(String::new(), |acc, byte: &u8| acc + &format!("{byte:02x}"));
                write!(f, "E'\\\\x{hex}'")
            }
            Value::Date(v) => write!(f, "'{v}'"),
            Value::Timestamp(v) => write!(f, "'{v}'"),
            Value::Null => write!(f, "NULL"),
            Value::Enum(_, v) => write!(f, "'{v}'"),
            Value::StringArray(v) => {
                let array_str = v
                    .iter()
                    .map(|s| format!("\"{}\"", s.replace('\"', "\\\"")))
                    .collect::<Vec<String>>()
                    .join(", ");
                write!(f, "'{{{array_str}}}'")
            }
        }
    }
}

impl FieldValue {
    pub fn value_data_type(&self) -> Option<DataType> {
        self.value.as_ref().map(|v| v.data_type())
    }
}
