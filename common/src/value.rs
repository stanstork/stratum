use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, fmt};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
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
        self.compare(other).map_or(false, |o| o == Ordering::Equal)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldValue {
    pub name: String,
    pub value: Option<Value>,
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Int(v) => write!(f, "{}", v),
            Value::Float(v) => write!(f, "{:.15}", v),
            Value::String(v) => write!(f, "'{}'", v.replace("'", "''")),
            Value::Boolean(v) => write!(f, "{}", v),
            Value::Json(v) => {
                let json_str = v.to_string().replace('\'', "''");
                write!(f, "'{}'", json_str)
            }
            Value::Uuid(v) => write!(f, "{}", v),
            Value::Bytes(v) => {
                let hex = v
                    .iter()
                    .fold(String::new(), |acc, byte| acc + &format!("{:02x}", byte));
                write!(f, "E'\\\\x{}'", hex)
            }
            Value::Date(v) => write!(f, "'{}'", v),
            Value::Timestamp(v) => write!(f, "'{}'", v),
        }
    }
}
