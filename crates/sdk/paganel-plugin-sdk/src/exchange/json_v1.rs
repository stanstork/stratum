use crate::{error::PluginError, value::Value};
use base64::Engine;
use chrono::{DateTime, NaiveDateTime};
use serde_json::{Value as JsonValue, json};

/// Encode a Value as the host-compatible `{"type": ..., "value": ...}` object.
pub fn value_to_json(value: &Value) -> JsonValue {
    match value {
        Value::Null => json!({ "type": "null" }),
        Value::Boolean(b) => json!({ "type": "bool", "value": b }),
        Value::Int(i) => json!({ "type": "i64", "value": i }),
        Value::UInt(u) => json!({ "type": "u64", "value": u }),
        Value::Float(f) => json!({ "type": "f64", "value": f }),
        Value::Decimal(d) => json!({ "type": "decimal", "value": d.to_string() }),
        Value::String(s) => json!({ "type": "string", "value": s }),
        Value::Binary(b) => {
            let b64 = base64::engine::general_purpose::STANDARD.encode(b);
            json!({ "type": "bytes", "value": b64 })
        }
        Value::Date(d) => json!({ "type": "date", "value": d.to_string() }),
        Value::Time { value } => json!({ "type": "time", "value": value.to_string() }),
        Value::Timestamp { value, offset_secs } => {
            let s = match offset_secs {
                Some(0) => format!("{}Z", value),
                Some(off) => {
                    let h = off / 3600;
                    let m = (off.abs() % 3600) / 60;
                    let sign = if *off >= 0 { '+' } else { '-' };
                    format!("{}{}{:02}:{:02}", value, sign, h.abs(), m)
                }
                None => value.to_string(),
            };
            json!({ "type": "timestamp", "value": s })
        }
        Value::Uuid(u) => json!({ "type": "uuid", "value": u.to_string() }),
        Value::Json(j) => json!({ "type": "json", "value": j }),
    }
}

/// Decode a `{"type": ..., "value": ...}` object into a Value.
pub fn json_to_value(json: &JsonValue) -> Result<Value, PluginError> {
    let type_str = json
        .get("type")
        .and_then(|v| v.as_str())
        .ok_or_else(|| PluginError::invalid_input("missing 'type' field in typed value"))?;

    let val = json.get("value");

    Ok(match type_str {
        "null" => Value::Null,
        "bool" => Value::Boolean(
            val.and_then(|v| v.as_bool())
                .ok_or_else(|| PluginError::invalid_input("expected bool"))?,
        ),
        "i64" => Value::Int(
            val.and_then(|v| v.as_i64())
                .ok_or_else(|| PluginError::invalid_input("expected i64"))?,
        ),
        "u64" => Value::UInt(
            val.and_then(|v| v.as_u64())
                .ok_or_else(|| PluginError::invalid_input("expected u64"))?,
        ),
        "f64" => Value::Float(
            val.and_then(|v| v.as_f64())
                .ok_or_else(|| PluginError::invalid_input("expected f64"))?,
        ),
        "string" => Value::String(
            val.and_then(|v| v.as_str())
                .ok_or_else(|| PluginError::invalid_input("expected string"))?
                .to_string(),
        ),
        "decimal" => {
            let s = val
                .and_then(|v| v.as_str())
                .ok_or_else(|| PluginError::invalid_input("expected decimal string"))?;
            Value::Decimal(
                s.parse()
                    .map_err(|_| PluginError::invalid_input(format!("invalid decimal: {}", s)))?,
            )
        }
        "bytes" => {
            let s = val
                .and_then(|v| v.as_str())
                .ok_or_else(|| PluginError::invalid_input("expected base64 string"))?;
            Value::Binary(
                base64::engine::general_purpose::STANDARD
                    .decode(s)
                    .map_err(|_| PluginError::invalid_input("invalid base64"))?,
            )
        }
        "date" => {
            let s = val
                .and_then(|v| v.as_str())
                .ok_or_else(|| PluginError::invalid_input("expected date string"))?;
            Value::Date(
                s.parse()
                    .map_err(|_| PluginError::invalid_input(format!("invalid date: {}", s)))?,
            )
        }
        "timestamp" => parse_timestamp(val)?,
        "uuid" => {
            let s = val
                .and_then(|v| v.as_str())
                .ok_or_else(|| PluginError::invalid_input("expected uuid string"))?;
            Value::Uuid(
                s.parse()
                    .map_err(|_| PluginError::invalid_input(format!("invalid uuid: {}", s)))?,
            )
        }
        "json" => Value::Json(val.cloned().unwrap_or(JsonValue::Null)),
        other => {
            return Err(PluginError::invalid_input(format!(
                "unknown type: {}",
                other
            )));
        }
    })
}

fn parse_timestamp(val: Option<&JsonValue>) -> Result<Value, PluginError> {
    let s = val
        .and_then(|v| v.as_str())
        .ok_or_else(|| PluginError::invalid_input("expected timestamp string"))?;

    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(Value::Timestamp {
            value: dt.naive_utc(),
            offset_secs: Some(dt.offset().local_minus_utc()),
        });
    }
    if let Some(naive_str) = s.strip_suffix('Z') {
        let naive: NaiveDateTime = naive_str
            .parse()
            .map_err(|_| PluginError::invalid_input(format!("invalid timestamp: {}", s)))?;
        return Ok(Value::Timestamp {
            value: naive,
            offset_secs: Some(0),
        });
    }
    let naive: NaiveDateTime = s
        .parse()
        .map_err(|_| PluginError::invalid_input(format!("invalid timestamp: {}", s)))?;
    Ok(Value::Timestamp {
        value: naive,
        offset_secs: None,
    })
}
