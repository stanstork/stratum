use crate::core::data_type::DataType;
use bigdecimal::{BigDecimal, FromPrimitive, ToPrimitive, Zero};
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, fmt, hash::Hash, str::FromStr};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Value {
    SmallInt(i16),
    Int32(i32),
    Int(i64),
    Uint(u64),
    Usize(usize),
    Float(f64),
    Decimal(BigDecimal),
    String(String),
    Boolean(bool),
    Json(serde_json::Value),
    Uuid(Uuid),
    Bytes(Vec<u8>),
    Date(NaiveDate),
    Timestamp(DateTime<Utc>),
    TimestampNaive(NaiveDateTime),
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
            SmallInt(v) => v.hash(state),
            Int32(v) => v.hash(state),
            Decimal(v) => v.to_string().hash(state),
            Int(v) => v.hash(state),
            Uint(v) => v.hash(state),
            Usize(v) => v.hash(state),
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
            TimestampNaive(v) => v.hash(state),
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
            Value::SmallInt(v) => Some(*v as f64),
            Value::Int32(v) => Some(*v as f64),
            Value::Int(v) => Some(*v as f64),
            Value::Uint(v) => Some(*v as f64),
            Value::Usize(v) => Some(*v as f64),
            Value::Float(v) => Some(*v),
            Value::Decimal(v) => v.to_f64(),
            Value::String(v) => v.parse::<f64>().ok(),
            Value::Boolean(v) => Some(if *v { 1.0 } else { 0.0 }),
            Value::Json(v) => v.as_f64(),
            Value::Uuid(_) => None,
            Value::Bytes(_) => None,
            Value::Date(_) => None,
            Value::Timestamp(_) => None,
            Value::TimestampNaive(_) => None,
            Value::Null => None,
            Value::Enum(_, _) => None,
            Value::StringArray(_) => None,
        }
    }

    pub fn as_usize(&self) -> Option<usize> {
        match self {
            Value::SmallInt(v) => Some(*v as usize),
            Value::Int32(v) => Some(*v as usize),
            Value::Int(v) => Some(*v as usize),
            Value::Uint(v) => Some(*v as usize),
            Value::Usize(v) => Some(*v),
            Value::Float(v) => Some(*v as usize),
            Value::Decimal(v) => v.to_usize(),
            Value::String(v) => v.parse::<usize>().ok(),
            Value::Boolean(v) => Some(if *v { 1 } else { 0 }),
            Value::Json(v) => v.as_u64().map(|u| u as usize),
            Value::Uuid(_) => None,
            Value::Bytes(_) => None,
            Value::Date(_) => None,
            Value::Timestamp(_) => None,
            Value::TimestampNaive(_) => None,
            Value::Null => None,
            Value::Enum(_, _) => None,
            Value::StringArray(_) => None,
        }
    }

    pub fn as_string(&self) -> Option<String> {
        match self {
            Value::SmallInt(v) => Some(v.to_string()),
            Value::Int32(v) => Some(v.to_string()),
            Value::Int(v) => Some(v.to_string()),
            Value::Uint(v) => Some(v.to_string()),
            Value::Usize(v) => Some(v.to_string()),
            Value::Float(v) => Some(v.to_string()),
            Value::Decimal(v) => Some(v.to_string()),
            Value::String(v) => Some(v.clone()),
            Value::Boolean(v) => Some(v.to_string()),
            Value::Json(v) => v.as_str().map(|s| s.to_string()),
            Value::Uuid(v) => Some(v.to_string()),
            Value::Bytes(_) => None,
            Value::Date(v) => Some(v.to_string()),
            Value::Timestamp(v) => Some(v.to_rfc3339()),
            Value::TimestampNaive(v) => Some(v.to_string()),
            Value::Null => Some("NULL".to_string()),
            Value::Enum(_, v) => Some(v.clone()),
            Value::StringArray(v) => Some(format!("{v:?}")),
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::SmallInt(v) => Some(*v != 0),
            Value::Int32(v) => Some(*v != 0),
            Value::Int(v) => Some(*v != 0),
            Value::Uint(v) => Some(*v != 0),
            Value::Usize(v) => Some(*v != 0),
            Value::Float(v) => Some(*v != 0.0),
            Value::Decimal(v) => Some(!v.is_zero()),
            Value::String(v) => match v.to_lowercase().as_str() {
                "true" | "1" => Some(true),
                "false" | "0" => Some(false),
                _ => None,
            },
            Value::Boolean(v) => Some(*v),
            Value::Json(v) => v.as_bool(),
            Value::Uuid(_) => None,
            Value::Bytes(_) => None,
            Value::Date(_) => None,
            Value::Timestamp(_) => None,
            Value::TimestampNaive(_) => None,
            Value::Null => None,
            Value::Enum(_, _) => None,
            Value::StringArray(_) => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::SmallInt(v) => Some(*v as i64),
            Value::Int32(v) => Some(*v as i64),
            Value::Int(v) => Some(*v),
            Value::Decimal(v) => v.to_i64(),
            Value::Uint(v) => i64::try_from(*v).ok(),
            Value::Usize(v) => i64::try_from(*v).ok(),
            _ => None,
        }
    }

    pub fn as_i32(&self) -> Option<i32> {
        match self {
            Value::SmallInt(v) => Some(*v as i32),
            Value::Int32(v) => Some(*v),
            Value::Int(v) => i32::try_from(*v).ok(),
            Value::Decimal(v) => v.to_i32(),
            Value::Uint(v) => i32::try_from(*v).ok(),
            Value::Usize(v) => i32::try_from(*v).ok(),
            _ => None,
        }
    }

    pub fn as_i16(&self) -> Option<i16> {
        match self {
            Value::SmallInt(v) => Some(*v),
            Value::Int32(v) => i16::try_from(*v).ok(),
            Value::Int(v) => i16::try_from(*v).ok(),
            Value::Decimal(v) => v.to_i16(),
            Value::Uint(v) => i16::try_from(*v).ok(),
            Value::Usize(v) => i16::try_from(*v).ok(),
            _ => None,
        }
    }

    pub fn as_i128(&self) -> Option<i128> {
        match self {
            Value::SmallInt(v) => Some(*v as i128),
            Value::Int32(v) => Some(*v as i128),
            Value::Int(v) => Some(*v as i128),
            Value::Decimal(v) => v.to_i128(),
            Value::Uint(v) => i128::try_from(*v).ok(),
            Value::Usize(v) => i128::try_from(*v).ok(),
            _ => None,
        }
    }

    pub fn as_big_decimal(&self) -> Option<BigDecimal> {
        match self {
            Value::Decimal(v) => Some(v.clone()),
            Value::SmallInt(v) => Some(BigDecimal::from(*v)),
            Value::Int32(v) => Some(BigDecimal::from(*v)),
            Value::Int(v) => Some(BigDecimal::from(*v)),
            Value::Uint(v) => BigDecimal::from_f64(*v as f64),
            Value::Usize(v) => BigDecimal::from_f64(*v as f64),
            Value::Float(v) => BigDecimal::from_f64(*v),
            Value::String(s) => BigDecimal::from_str(s).ok(),
            _ => None,
        }
    }

    pub fn compare(&self, other: &Value) -> Option<Ordering> {
        if let (Some(a), Some(b)) = (self.as_big_decimal(), other.as_big_decimal()) {
            return Some(a.cmp(&b));
        }

        if let (Some(a), Some(b)) = (self.as_i128(), other.as_i128()) {
            return Some(a.cmp(&b));
        }

        use Value::*;
        match (self, other) {
            (Float(a), Float(b)) => a.partial_cmp(b),
            (Float(a), _) => other.as_f64().and_then(|b| a.partial_cmp(&b)),
            (_, Float(b)) => self.as_f64().and_then(|a| a.partial_cmp(b)),
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
            Value::SmallInt(_) => DataType::Short,
            Value::Int32(_) => DataType::Int,
            Value::Int(_) => DataType::Int,
            Value::Uint(_) => DataType::IntUnsigned,
            Value::Usize(_) => DataType::IntUnsigned,
            Value::Float(_) => DataType::Float,
            Value::Decimal(_) => DataType::Decimal,
            Value::String(_) => DataType::String,
            Value::Boolean(_) => DataType::Boolean,
            Value::Json(_) => DataType::Json,
            Value::Uuid(_) => DataType::VarChar, // UUIDs are typically stored as strings
            Value::Bytes(_) => DataType::Bytea,
            Value::Date(_) => DataType::Date,
            Value::Timestamp(_) => DataType::TimestampTz,
            Value::TimestampNaive(_) => DataType::Timestamp,
            Value::Enum(_, _) => DataType::Enum,
            Value::StringArray(_) => DataType::Array(None),
            Value::Null => DataType::Null,
        }
    }

    pub fn size_bytes(&self) -> usize {
        match self {
            Value::SmallInt(_) => std::mem::size_of::<i16>(),
            Value::Int32(_) => std::mem::size_of::<i32>(),
            Value::Int(_) => std::mem::size_of::<i64>(),
            Value::Uint(_) => std::mem::size_of::<u64>(),
            Value::Usize(_) => std::mem::size_of::<usize>(),
            Value::Float(_) => std::mem::size_of::<f64>(),
            Value::Decimal(v) => v.to_string().len(),
            Value::String(s) => s.len(),
            Value::Boolean(_) => std::mem::size_of::<bool>(),
            Value::Json(v) => serde_json::to_string(v).map_or(0, |s| s.len()),
            Value::Uuid(_) => 16, // UUIDs are 16 bytes
            Value::Bytes(b) => b.len(),
            Value::Date(_) => std::mem::size_of::<NaiveDate>(),
            Value::Timestamp(_) => std::mem::size_of::<DateTime<Utc>>(),
            Value::TimestampNaive(_) => std::mem::size_of::<NaiveDateTime>(),
            Value::Enum(_, v) => v.len(),
            Value::StringArray(arr) => arr.iter().map(|s| s.len()).sum(),
            Value::Null => 0,
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
            Value::SmallInt(v) => write!(f, "{v}"),
            Value::Int32(v) => write!(f, "{v}"),
            Value::Int(v) => write!(f, "{v}"),
            Value::Uint(v) => write!(f, "{v}"),
            Value::Usize(v) => write!(f, "{v}"),
            Value::Float(v) => write!(f, "{v:.15}"),
            Value::Decimal(v) => write!(f, "{}", v),
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
            Value::TimestampNaive(v) => write!(f, "'{v}'"),
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
