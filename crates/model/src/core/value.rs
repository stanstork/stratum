use crate::core::types::{FloatSize, IntSize, Type};
use bigdecimal::BigDecimal;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use serde::{Deserialize, Serialize};
use std::net::IpAddr;
use uuid::Uuid;

/// Canonical value representation matching the Type enum
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    // Numeric
    Int(i64),
    UInt(u64),
    Decimal(BigDecimal),
    Float(f64),

    // String
    String(String),

    // Binary
    Binary(Vec<u8>),

    // Temporal
    Date(NaiveDate),
    Time {
        value: NaiveTime,
        offset_secs: Option<i32>,
    },
    Timestamp {
        value: NaiveDateTime,
        offset_secs: Option<i32>,
    },
    Interval(IntervalValue),
    Year(i16),

    // Scalar
    Boolean(bool),
    Uuid(Uuid),
    Json(serde_json::Value),
    Bits(Vec<bool>),

    // Complex
    Array(Vec<Value>),
    Enum {
        type_name: String,
        value: String,
    },
    Set(Vec<String>),
    Geometry(Vec<u8>), // WKB format

    // Network
    IpAddr(IpAddr),
    Cidr {
        addr: IpAddr,
        prefix: u8,
    },
    MacAddr([u8; 6]),

    // Composite
    Composite {
        type_name: String,
        fields: Vec<(String, Value)>,
    },

    // Null
    Null,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct IntervalValue {
    pub months: i32,
    pub days: i32,
    pub microseconds: i64,
}

impl IntervalValue {
    pub fn new(months: i32, days: i32, microseconds: i64) -> Self {
        Self {
            months,
            days,
            microseconds,
        }
    }

    pub fn from_hms(hours: i64, minutes: i64, seconds: i64) -> Self {
        let microseconds = (hours * 3600 + minutes * 60 + seconds) * 1_000_000;
        Self {
            months: 0,
            days: 0,
            microseconds,
        }
    }

    pub fn from_ymd(years: i32, months: i32, days: i32) -> Self {
        Self {
            months: years * 12 + months,
            days,
            microseconds: 0,
        }
    }
}

impl Value {
    pub fn size_bytes(&self) -> usize {
        match self {
            Value::Int(_) => std::mem::size_of::<i64>(),
            Value::UInt(_) => std::mem::size_of::<u64>(),
            Value::Decimal(d) => d.to_string().len(),
            Value::Float(_) => std::mem::size_of::<f64>(),
            Value::String(s) => s.len(),
            Value::Binary(b) => b.len(),
            Value::Date(_) => std::mem::size_of::<NaiveDate>(),
            Value::Time { .. } => {
                std::mem::size_of::<NaiveTime>() + std::mem::size_of::<Option<i32>>()
            }
            Value::Timestamp { .. } => {
                std::mem::size_of::<NaiveDateTime>() + std::mem::size_of::<Option<i32>>()
            }
            Value::Interval(_) => std::mem::size_of::<IntervalValue>(),
            Value::Year(_) => std::mem::size_of::<i16>(),
            Value::Boolean(_) => std::mem::size_of::<bool>(),
            Value::Uuid(_) => std::mem::size_of::<Uuid>(),
            Value::Json(j) => j.to_string().len(),
            Value::Bits(b) => b.len(),
            Value::Array(arr) => arr.iter().map(|v| v.size_bytes()).sum(),
            Value::Enum { type_name, value } => type_name.len() + value.len(),
            Value::Set(s) => s.iter().map(|v| v.len()).sum(),
            Value::Geometry(g) => g.len(),
            Value::IpAddr(_) => std::mem::size_of::<IpAddr>(),
            Value::Cidr { .. } => std::mem::size_of::<IpAddr>() + std::mem::size_of::<u8>(),
            Value::MacAddr(_) => 6,
            Value::Composite { type_name, fields } => {
                let fields_size: usize = fields
                    .iter()
                    .map(|(name, value)| name.len() + value.size_bytes())
                    .sum();
                type_name.len() + fields_size
            }
            Value::Null => 0,
        }
    }

    pub fn as_string(&self) -> Option<String> {
        match self {
            Value::String(s) => Some(s.clone()),
            Value::Int(v) => Some(v.to_string()),
            Value::UInt(v) => Some(v.to_string()),
            Value::Float(v) => Some(v.to_string()),
            Value::Decimal(v) => Some(v.to_string()),
            Value::Boolean(v) => Some(v.to_string()),
            Value::Date(v) => Some(v.to_string()),
            Value::Timestamp { value, .. } => Some(value.to_string()),
            Value::Uuid(v) => Some(v.to_string()),
            Value::Json(v) => Some(v.to_string()),
            Value::Enum { value, .. } => Some(value.clone()),
            Value::Null => None,
            _ => Some(format!("{:?}", self)),
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Int(v) => Some(*v as f64),
            Value::UInt(v) => Some(*v as f64),
            Value::Float(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int(v) => Some(*v),
            Value::UInt(v) => i64::try_from(*v).ok(),
            Value::Float(v) => Some(*v as i64),
            _ => None,
        }
    }

    pub fn as_i32(&self) -> Option<i32> {
        self.as_i64().and_then(|v| i32::try_from(v).ok())
    }

    pub fn as_usize(&self) -> Option<usize> {
        self.as_i64().and_then(|v| usize::try_from(v).ok())
    }

    pub fn data_type(&self) -> Type {
        match self {
            Value::Int(_) => Type::Int {
                bits: IntSize::I64,
                unsigned: false,
                auto_increment: false,
            },
            Value::UInt(_) => Type::Int {
                bits: IntSize::I64,
                unsigned: true,
                auto_increment: false,
            },
            Value::Float(_) => Type::Float {
                bits: FloatSize::F64,
            },
            Value::Decimal(d) => {
                // Try to infer precision/scale from the decimal value
                let s = d.to_string();
                Type::Decimal {
                    precision: Some(s.len() as u8),
                    scale: s.split('.').nth(1).map(|s| s.len() as u8),
                }
            }
            Value::String(s) => Type::Varchar {
                length: Some(s.len()),
                charset: None,
            },
            Value::Binary(_) => Type::Varbinary { length: None },
            Value::Boolean(_) => Type::Boolean,
            Value::Date(_) => Type::Date,
            Value::Time { .. } => Type::Time {
                precision: None,
                with_tz: false,
            },
            Value::Timestamp { .. } => Type::Timestamp {
                precision: None,
                with_tz: false,
            },
            Value::Interval(_) => Type::Interval { fields: None },
            Value::Year(_) => Type::Year,
            Value::Uuid(_) => Type::Uuid,
            Value::Json(_) => Type::Json { binary: false },
            Value::Bits(_) => Type::Bit { length: None },
            Value::Array(_) => Type::Array {
                element: Box::new(Type::Text { charset: None }),
            },
            Value::Enum { type_name, .. } => Type::Enum {
                name: type_name.clone(),
                values: vec![],
            },
            Value::Set(_) => Type::Set { values: vec![] },
            Value::Geometry(_) => Type::Geometry {
                kind: None,
                srid: None,
            },
            Value::IpAddr(_) => Type::Inet,
            Value::Cidr { .. } => Type::Cidr,
            Value::MacAddr(_) => Type::MacAddr,
            Value::Composite { type_name, .. } => Type::Composite {
                name: type_name.clone(),
                fields: vec![],
            },
            Value::Null => Type::Text { charset: None }, // Default for NULL
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldValue {
    pub name: String,
    pub value: Option<Value>,
    pub data_type: Type,
}
