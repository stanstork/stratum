use bigdecimal::BigDecimal;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use uuid::Uuid;

/// Guest-side mirror of the canonical value.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Boolean(bool),
    Int(i64),
    UInt(u64),
    Float(f64),
    Decimal(BigDecimal),
    String(String),
    Binary(Vec<u8>),
    Date(NaiveDate),
    Time {
        value: NaiveTime,
    },
    Timestamp {
        value: NaiveDateTime,
        offset_secs: Option<i32>,
    },
    Uuid(Uuid),
    Json(serde_json::Value),
}

impl Value {
    pub fn type_tag(&self) -> &'static str {
        match self {
            Value::Null => "null",
            Value::Boolean(_) => "bool",
            Value::Int(_) => "i64",
            Value::UInt(_) => "u64",
            Value::Float(_) => "f64",
            Value::Decimal(_) => "decimal",
            Value::String(_) => "string",
            Value::Binary(_) => "bytes",
            Value::Date(_) => "date",
            Value::Time { .. } => "time",
            Value::Timestamp { .. } => "timestamp",
            Value::Uuid(_) => "uuid",
            Value::Json(_) => "json",
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    // Convenience constructors used in tests and examples.
    pub fn string(s: impl Into<String>) -> Self {
        Value::String(s.into())
    }
    pub fn int(i: i64) -> Self {
        Value::Int(i)
    }
    pub fn float(f: f64) -> Self {
        Value::Float(f)
    }
    pub fn bool(b: bool) -> Self {
        Value::Boolean(b)
    }
}

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Boolean(v)
    }
}

impl From<i32> for Value {
    fn from(v: i32) -> Self {
        Value::Int(v as i64)
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Int(v)
    }
}

impl From<u64> for Value {
    fn from(v: u64) -> Self {
        Value::UInt(v)
    }
}

impl From<f32> for Value {
    fn from(v: f32) -> Self {
        Value::Float(v as f64)
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Float(v)
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::String(v.into())
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::String(v)
    }
}

impl<T: Into<Value>> From<Option<T>> for Value {
    fn from(v: Option<T>) -> Self {
        match v {
            Some(inner) => inner.into(),
            None => Value::Null,
        }
    }
}
