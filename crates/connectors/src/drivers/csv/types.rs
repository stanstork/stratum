use bigdecimal::BigDecimal;
use chrono::{NaiveDate, NaiveDateTime};
use model::core::{
    types::{FloatSize, IntSize, Type},
    value::Value,
};
use std::str::FromStr;

/// The promotion sequence: start at the current type and widen until it fits.
fn chain() -> Vec<Type> {
    vec![
        Type::Int {
            bits: IntSize::I16,
            unsigned: false,
            auto_increment: false,
        },
        Type::Int {
            bits: IntSize::I32,
            unsigned: false,
            auto_increment: false,
        },
        Type::Int {
            bits: IntSize::I64,
            unsigned: false,
            auto_increment: false,
        },
        Type::Decimal {
            precision: None,
            scale: None,
        },
        Type::Float {
            bits: FloatSize::F32,
        },
        Type::Float {
            bits: FloatSize::F64,
        },
        Type::Boolean,
        Type::Date,
        Type::Timestamp {
            precision: None,
            with_tz: false,
        },
        Type::Json { binary: false },
        Type::Text { charset: None },
    ]
}

/// Check if type can parse the given string.
fn can_parse(data_type: &Type, value: &str) -> bool {
    if value.is_empty() {
        return true; // treat empty as null
    }
    match data_type {
        Type::Int {
            bits: IntSize::I16, ..
        } => value.parse::<i16>().is_ok(),
        Type::Int {
            bits: IntSize::I32, ..
        } => value.parse::<i32>().is_ok(),
        Type::Int {
            bits: IntSize::I64, ..
        } => value.parse::<i64>().is_ok(),
        Type::Decimal { .. } => BigDecimal::from_str(value).is_ok(),
        Type::Float {
            bits: FloatSize::F32,
        } => value.parse::<f32>().is_ok(),
        Type::Float {
            bits: FloatSize::F64,
        } => value.parse::<f64>().is_ok(),
        Type::Boolean => matches!(value.to_lowercase().as_str(), "true" | "false" | "1" | "0"),
        Type::Date => NaiveDate::parse_from_str(value, "%Y-%m-%d").is_ok(),
        Type::Timestamp { .. } => {
            NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S").is_ok()
                || NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S").is_ok()
                || chrono::DateTime::parse_from_rfc3339(value).is_ok()
        }
        Type::Json { .. } => serde_json::from_str::<serde_json::Value>(value).is_ok(),
        Type::Text { .. } | Type::Varchar { .. } => true,
        _ => false,
    }
}

pub trait CsvType {
    fn promote(&self, value: &str) -> Type;
    fn data_type(&self) -> Type;
    fn get_value(&self, value: &str) -> Option<Value>;
}

impl CsvType for Type {
    fn promote(&self, value: &str) -> Type {
        let chain = chain();
        // Find our index in the promotion chain (fallback to start)
        let start = chain
            .iter()
            .position(|t| self.is_same_base_type(t))
            .unwrap_or(0);
        // Find the first type from here onward that can parse the value
        chain[start..]
            .iter()
            .find(|t| can_parse(t, value))
            .cloned()
            .unwrap_or(Type::Text { charset: None })
    }

    fn data_type(&self) -> Type {
        self.clone()
    }

    fn get_value(&self, value: &str) -> Option<Value> {
        if value.is_empty() {
            return None;
        }

        match self {
            Type::Int { bits, unsigned, .. } => {
                if *unsigned {
                    match bits {
                        IntSize::I8 => value.parse::<u8>().ok().map(|v| Value::UInt(v as u64)),
                        IntSize::I16 => value.parse::<u16>().ok().map(|v| Value::UInt(v as u64)),
                        IntSize::I24 | IntSize::I32 => {
                            value.parse::<u32>().ok().map(|v| Value::UInt(v as u64))
                        }
                        IntSize::I64 => value.parse::<u64>().ok().map(Value::UInt),
                    }
                } else {
                    match bits {
                        IntSize::I8 => value.parse::<i8>().ok().map(|v| Value::Int(v as i64)),
                        IntSize::I16 => value.parse::<i16>().ok().map(|v| Value::Int(v as i64)),
                        IntSize::I24 | IntSize::I32 => {
                            value.parse::<i32>().ok().map(|v| Value::Int(v as i64))
                        }
                        IntSize::I64 => value.parse::<i64>().ok().map(Value::Int),
                    }
                }
            }

            Type::Float { bits } => match bits {
                FloatSize::F32 => value.parse::<f32>().ok().map(|v| Value::Float(v as f64)),
                FloatSize::F64 => value.parse::<f64>().ok().map(Value::Float),
            },

            Type::Decimal { .. } => value.parse::<BigDecimal>().ok().map(Value::Decimal),

            Type::Text { .. } | Type::Varchar { .. } | Type::Char { .. } => {
                Some(Value::String(value.to_string()))
            }

            Type::Enum { .. } => Some(Value::String(value.to_string())),

            Type::Boolean => match value.to_lowercase().as_str() {
                "true" | "1" => Some(Value::Boolean(true)),
                "false" | "0" => Some(Value::Boolean(false)),
                _ => None,
            },

            Type::Json { .. } => serde_json::from_str::<serde_json::Value>(value)
                .ok()
                .map(Value::Json),

            Type::Timestamp { with_tz, .. } => {
                // Try multiple timestamp formats
                if let Ok(dt) = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S") {
                    return Some(Value::Timestamp {
                        value: dt,
                        offset_secs: if *with_tz { Some(0) } else { None },
                    });
                }
                if let Ok(dt) = NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S") {
                    return Some(Value::Timestamp {
                        value: dt,
                        offset_secs: if *with_tz { Some(0) } else { None },
                    });
                }
                if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(value) {
                    return Some(Value::Timestamp {
                        value: dt.naive_utc(),
                        offset_secs: Some(dt.offset().local_minus_utc()),
                    });
                }
                None
            }

            Type::Date => NaiveDate::parse_from_str(value, "%Y-%m-%d")
                .ok()
                .map(Value::Date),

            Type::Year => value.parse::<i16>().ok().map(Value::Year),

            _ => None,
        }
    }
}

/// Helper trait to compare Type base types ignoring parameters
trait TypeBaseComparison {
    fn is_same_base_type(&self, other: &Type) -> bool;
}

impl TypeBaseComparison for Type {
    fn is_same_base_type(&self, other: &Type) -> bool {
        std::mem::discriminant(self) == std::mem::discriminant(other)
    }
}
