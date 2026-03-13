use bigdecimal::BigDecimal;
use chrono::{DateTime, NaiveDateTime};
use model::core::{types::Type, value::Value};
use uuid::Uuid;

/// Coerce a value to match the target canonical type.
/// This handles conversions like String -> Int, Binary -> Text, etc.
pub(crate) fn coerce_value(value: Value, target_type: &Type) -> Value {
    let value = coerce_numeric(value, target_type);
    let value = coerce_temporal(value, target_type);
    let value = coerce_enum(value, target_type);
    let value = coerce_uuid(value, target_type);
    coerce_text(value, target_type)
}

fn coerce_uuid(value: Value, target_type: &Type) -> Value {
    if let Type::Uuid = target_type
        && let Value::String(ref s) = value
        && let Ok(uuid) = Uuid::parse_str(s)
    {
        return Value::Uuid(uuid);
    }
    value
}

fn coerce_numeric(value: Value, target_type: &Type) -> Value {
    match target_type {
        // Signed integers
        Type::Int {
            unsigned: false, ..
        } => {
            if let Some(v) = extract_i64(&value) {
                return Value::Int(v);
            }
        }

        // Unsigned integers
        Type::Int { unsigned: true, .. } => {
            if let Some(v) = extract_u64(&value) {
                return Value::UInt(v);
            }
        }

        // Year
        Type::Year => {
            if let Some(v) = extract_i64(&value) {
                return Value::Year(v as i16);
            }
        }

        // Decimal
        Type::Decimal { .. } => {
            if let Some(v) = extract_decimal(&value) {
                return Value::Decimal(v);
            }
        }

        // Float
        Type::Float { .. } => {
            if let Some(v) = extract_f64(&value) {
                return Value::Float(v);
            }
        }

        _ => {}
    }

    value
}

fn extract_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Int(v) => Some(*v),
        Value::UInt(v) => i64::try_from(*v).ok(),
        Value::Float(v) => Some(*v as i64),
        Value::Decimal(d) => d.to_string().parse().ok(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

fn extract_u64(value: &Value) -> Option<u64> {
    match value {
        Value::Int(v) => u64::try_from(*v).ok(),
        Value::UInt(v) => Some(*v),
        Value::Float(v) => Some(*v as u64),
        Value::Decimal(d) => d.to_string().parse().ok(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

fn extract_decimal(value: &Value) -> Option<BigDecimal> {
    match value {
        Value::Decimal(d) => Some(d.clone()),
        Value::Int(v) => Some(BigDecimal::from(*v)),
        Value::UInt(v) => Some(BigDecimal::from(*v as i64)),
        Value::Float(v) => BigDecimal::try_from(*v).ok(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

fn extract_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Float(v) => Some(*v),
        Value::Int(v) => Some(*v as f64),
        Value::UInt(v) => Some(*v as f64),
        Value::Decimal(d) => d.to_string().parse().ok(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

fn coerce_text(value: Value, target_type: &Type) -> Value {
    match target_type {
        Type::Text { .. } | Type::Varchar { .. } | Type::Char { .. } => {
            if let Value::Binary(bytes) = value {
                return match String::from_utf8(bytes) {
                    Ok(text) => Value::String(text),
                    Err(err) => {
                        let bytes = err.into_bytes();
                        Value::String(String::from_utf8_lossy(&bytes).to_string())
                    }
                };
            }
        }
        _ => {}
    }

    value
}

fn coerce_enum(value: Value, target_type: &Type) -> Value {
    match target_type {
        Type::Enum { name, .. } => match value {
            Value::Enum { value: v, .. } => Value::Enum {
                type_name: name.clone(),
                value: v,
            },
            Value::String(s) => Value::Enum {
                type_name: name.clone(),
                value: s,
            },
            other => other,
        },

        // Domain types - preserve enum value with domain name
        Type::Domain { name, .. } => {
            if let Value::Enum { value: v, .. } = value {
                return Value::Enum {
                    type_name: name.clone(),
                    value: v,
                };
            }
            value
        }

        _ => value,
    }
}

fn coerce_temporal(value: Value, target_type: &Type) -> Value {
    match target_type {
        // Timestamp without timezone (DATETIME in MySQL)
        Type::Timestamp { with_tz: false, .. } => match value {
            Value::Timestamp {
                value: ts,
                offset_secs: Some(_),
            } => Value::Timestamp {
                value: ts,
                offset_secs: None,
            },
            Value::Timestamp { .. } => value,
            Value::String(ref s) => match parse_naive_datetime(s) {
                Some(dt) => Value::Timestamp {
                    value: dt,
                    offset_secs: None,
                },
                None => value,
            },
            _ => value,
        },

        // Timestamp with timezone (TIMESTAMP in MySQL, stored as UTC)
        Type::Timestamp { with_tz: true, .. } => match value {
            Value::Timestamp {
                offset_secs: Some(_),
                ..
            } => value,
            Value::Timestamp {
                value: ts,
                offset_secs: None,
            } => Value::Timestamp {
                value: ts,
                offset_secs: Some(0), // Assume UTC
            },
            Value::String(ref s) => match parse_datetime(s) {
                Some((dt, offset)) => Value::Timestamp {
                    value: dt,
                    offset_secs: Some(offset),
                },
                None => value,
            },
            _ => value,
        },

        _ => value,
    }
}

fn parse_datetime(raw: &str) -> Option<(NaiveDateTime, i32)> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(raw) {
        return Some((dt.naive_utc(), dt.offset().local_minus_utc()));
    }

    parse_naive_datetime(raw).map(|naive| (naive, 0))
}

fn parse_naive_datetime(raw: &str) -> Option<NaiveDateTime> {
    NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S"))
        .or_else(|_| NaiveDateTime::parse_from_str(raw, "%Y-%m-%dT%H:%M:%S%.f"))
        .or_else(|_| NaiveDateTime::parse_from_str(raw, "%Y-%m-%dT%H:%M:%S"))
        .ok()
        .or_else(|| {
            DateTime::parse_from_rfc3339(raw)
                .map(|dt| dt.naive_utc())
                .ok()
        })
}
