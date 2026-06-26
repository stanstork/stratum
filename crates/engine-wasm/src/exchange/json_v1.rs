use crate::{
    error::WasmError,
    exchange::types::{
        FilterDecision, PluginBatch, PluginInput, PluginOutput, SourcePage, WriteResult,
    },
    schema::PluginField,
};
use model::{
    core::value::{FieldValue, Value},
    records::{OpType, Record},
};
use serde_json::{Map, Value as JsonValue, json};
use std::collections::HashMap;

pub fn serialize_input(input: &PluginInput, schema: &[PluginField]) -> Result<Vec<u8>, WasmError> {
    let declared: HashMap<&str, &str> = schema
        .iter()
        .map(|f| (f.name.as_str(), f.field_type.as_str()))
        .collect();

    let mut map = Map::new();
    for (key, value) in input.fields() {
        let json = match declared.get(key.as_str()) {
            Some(tag) => value_to_json(&coerce_value(value, tag)),
            None => value_to_json(value),
        };
        map.insert(key.clone(), json);
    }
    serde_json::to_vec(&JsonValue::Object(map))
        .map_err(|e| WasmError::SerializationError(e.to_string()))
}

/// Coerce a value toward the plugin's declared input type tag.
fn coerce_value(value: &Value, tag: &str) -> Value {
    match tag {
        "f64" | "float" | "double" => match value {
            Value::Int(i) => Value::Float(*i as f64),
            Value::UInt(u) => Value::Float(*u as f64),
            Value::Decimal(d) => Value::Float(d.to_string().parse::<f64>().unwrap_or(f64::NAN)),
            _ => value.clone(),
        },
        "string" | "text" => match value {
            // Already-string and null pass through; other scalars stringify.
            Value::String(_) | Value::Null => value.clone(),
            Value::Boolean(b) => Value::String(b.to_string()),
            Value::Int(i) => Value::String(i.to_string()),
            Value::UInt(u) => Value::String(u.to_string()),
            Value::Float(f) => Value::String(f.to_string()),
            Value::Decimal(d) => Value::String(d.to_string()),
            _ => value.clone(),
        },
        _ => value.clone(),
    }
}

pub fn serialize_cursor(cursor: Option<&str>) -> Result<Vec<u8>, WasmError> {
    let json = match cursor {
        Some(c) => json!({ "cursor": c }),
        None => json!({ "cursor": null }),
    };
    serde_json::to_vec(&json).map_err(|e| WasmError::SerializationError(e.to_string()))
}

pub fn serialize_batch(batch: &PluginBatch) -> Result<Vec<u8>, WasmError> {
    let records: Vec<JsonValue> = batch
        .records
        .iter()
        .map(|record| {
            let mut row = Map::new();
            for field in &record.fields {
                let value = match &field.value {
                    Some(v) => value_to_json(v),
                    None => value_to_json(&Value::Null),
                };
                row.insert(field.name.clone(), value);
            }
            JsonValue::Object(row)
        })
        .collect();

    serde_json::to_vec(&json!({ "records": records }))
        .map_err(|e| WasmError::SerializationError(e.to_string()))
}

pub fn deserialize_output(bytes: &[u8], plugin: &str) -> Result<PluginOutput, WasmError> {
    let json: JsonValue = serde_json::from_slice(bytes)
        .map_err(|e| WasmError::DeserializationError(format!("{}: {}", plugin, e)))?;

    // Check for guest-side error
    if let Some(err) = json.get("error") {
        return Err(WasmError::PluginError {
            plugin: plugin.to_string(),
            message: err.as_str().unwrap_or("unknown error").to_string(),
        });
    }

    let value = json_to_value(&json, plugin)?;
    Ok(PluginOutput { value })
}

pub fn deserialize_filter_decision(
    bytes: &[u8],
    plugin: &str,
) -> Result<FilterDecision, WasmError> {
    let json: JsonValue = serde_json::from_slice(bytes)
        .map_err(|e| WasmError::DeserializationError(format!("{}: {}", plugin, e)))?;

    // Check for guest-side error
    if let Some(err) = json.get("error") {
        return Err(WasmError::PluginError {
            plugin: plugin.to_string(),
            message: err.as_str().unwrap_or("unknown error").to_string(),
        });
    }

    let pass =
        json.get("pass")
            .and_then(|v| v.as_bool())
            .ok_or_else(|| WasmError::InvalidOutput {
                plugin: plugin.to_string(),
                reason: "missing 'pass' boolean field".to_string(),
            })?;

    if pass {
        Ok(FilterDecision::Pass)
    } else {
        let reason = json
            .get("reason")
            .and_then(|v| v.as_str())
            .unwrap_or("rejected")
            .to_string();
        Ok(FilterDecision::Reject { reason })
    }
}

pub fn deserialize_source_page(bytes: &[u8], plugin: &str) -> Result<SourcePage, WasmError> {
    let json: JsonValue = serde_json::from_slice(bytes)
        .map_err(|e| WasmError::DeserializationError(format!("{}: {}", plugin, e)))?;

    // Check for guest-side error
    if let Some(err) = json.get("error") {
        return Err(WasmError::PluginError {
            plugin: plugin.to_string(),
            message: err.as_str().unwrap_or("unknown error").to_string(),
        });
    }

    let records_json = json
        .get("records")
        .and_then(|v| v.as_array())
        .ok_or_else(|| WasmError::InvalidOutput {
            plugin: plugin.to_string(),
            reason: "missing 'records' array".to_string(),
        })?;

    let mut records = Vec::with_capacity(records_json.len());
    for (i, row_json) in records_json.iter().enumerate() {
        let record = json_row_to_record(row_json, plugin, i)?;
        records.push(record);
    }

    let next_cursor = json
        .get("next_cursor")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let has_more = json
        .get("has_more")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    Ok(SourcePage {
        records,
        next_cursor,
        has_more,
    })
}

pub fn deserialize_write_result(bytes: &[u8], plugin: &str) -> Result<WriteResult, WasmError> {
    let json: JsonValue = serde_json::from_slice(bytes)
        .map_err(|e| WasmError::DeserializationError(format!("{}: {}", plugin, e)))?;

    // Check for guest-side error
    if let Some(err) = json.get("error") {
        return Err(WasmError::PluginError {
            plugin: plugin.to_string(),
            message: err.as_str().unwrap_or("unknown error").to_string(),
        });
    }

    let rows_written = json
        .get("rows_written")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| WasmError::InvalidOutput {
            plugin: plugin.to_string(),
            reason: "missing 'rows_written' integer".to_string(),
        })?;

    Ok(WriteResult { rows_written })
}

fn value_to_json(value: &Value) -> JsonValue {
    match value {
        Value::Null => json!({ "type": "null" }),
        Value::Boolean(b) => json!({ "type": "bool", "value": b }),
        Value::Int(i) => json!({ "type": "i64", "value": i }),
        Value::UInt(u) => json!({ "type": "u64", "value": u }),
        Value::Float(f) => json!({ "type": "f64", "value": f }),
        Value::Decimal(d) => json!({ "type": "decimal", "value": d.to_string() }),
        Value::String(s) => json!({ "type": "string", "value": s }),
        Value::Binary(b) => {
            use base64::Engine;
            let encoded = base64::engine::general_purpose::STANDARD.encode(b);
            json!({ "type": "bytes", "value": encoded })
        }
        Value::Date(d) => json!({ "type": "date", "value": d.to_string() }),
        Value::Timestamp {
            value: ts,
            offset_secs,
        } => {
            // ISO 8601 format
            let s = if let Some(offset) = offset_secs {
                format!("{}+{:02}:{:02}", ts, offset / 3600, (offset % 3600) / 60)
            } else {
                format!("{}Z", ts)
            };
            json!({ "type": "timestamp", "value": s })
        }
        Value::Time { value: t, .. } => json!({ "type": "time", "value": t.to_string() }),
        Value::Uuid(u) => json!({ "type": "uuid", "value": u.to_string() }),
        Value::Json(j) => json!({ "type": "json", "value": j }),
        Value::Enum { value: v, .. } => json!({ "type": "string", "value": v }),
        // For types that don't have a clean JSON representation, serialize as string
        other => json!({ "type": "string", "value": format!("{:?}", other) }),
    }
}

fn json_to_value(json: &JsonValue, plugin: &str) -> Result<Value, WasmError> {
    let type_str =
        json.get("type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| WasmError::InvalidOutput {
                plugin: plugin.to_string(),
                reason: "missing 'type' field in output value".to_string(),
            })?;

    let val = json.get("value");

    match type_str {
        "null" => Ok(Value::Null),
        "bool" => {
            let b = val
                .and_then(|v| v.as_bool())
                .ok_or_else(|| invalid(plugin, "bool value expected"))?;
            Ok(Value::Boolean(b))
        }
        "i64" => {
            let i = val
                .and_then(|v| v.as_i64())
                .ok_or_else(|| invalid(plugin, "i64 value expected"))?;
            Ok(Value::Int(i))
        }
        "u64" => {
            let u = val
                .and_then(|v| v.as_u64())
                .ok_or_else(|| invalid(plugin, "u64 value expected"))?;
            Ok(Value::UInt(u))
        }
        "f64" => {
            let f = val
                .and_then(|v| v.as_f64())
                .ok_or_else(|| invalid(plugin, "f64 value expected"))?;
            Ok(Value::Float(f))
        }
        "string" => {
            let s = val
                .and_then(|v| v.as_str())
                .ok_or_else(|| invalid(plugin, "string value expected"))?;
            Ok(Value::String(s.to_string()))
        }
        "decimal" => {
            let s = val
                .and_then(|v| v.as_str())
                .ok_or_else(|| invalid(plugin, "decimal string expected"))?;
            let d: bigdecimal::BigDecimal = s
                .parse()
                .map_err(|_| invalid(plugin, &format!("invalid decimal: {}", s)))?;
            Ok(Value::Decimal(d))
        }
        "date" => {
            let s = val
                .and_then(|v| v.as_str())
                .ok_or_else(|| invalid(plugin, "date string expected"))?;
            let d: chrono::NaiveDate = s
                .parse()
                .map_err(|_| invalid(plugin, &format!("invalid date: {}", s)))?;
            Ok(Value::Date(d))
        }
        "timestamp" => {
            let s = val
                .and_then(|v| v.as_str())
                .ok_or_else(|| invalid(plugin, "timestamp string expected"))?;
            // Parse ISO 8601. Try with timezone first, then naive.
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
                Ok(Value::Timestamp {
                    value: dt.naive_utc(),
                    offset_secs: Some(dt.offset().local_minus_utc()),
                })
            } else if let Some(naive_str) = s.strip_suffix('Z') {
                let naive: chrono::NaiveDateTime = naive_str
                    .parse()
                    .map_err(|_| invalid(plugin, &format!("invalid timestamp: {}", s)))?;
                Ok(Value::Timestamp {
                    value: naive,
                    offset_secs: Some(0),
                })
            } else {
                let naive: chrono::NaiveDateTime = s
                    .parse()
                    .map_err(|_| invalid(plugin, &format!("invalid timestamp: {}", s)))?;
                Ok(Value::Timestamp {
                    value: naive,
                    offset_secs: None,
                })
            }
        }
        "bytes" => {
            use base64::Engine;
            let s = val
                .and_then(|v| v.as_str())
                .ok_or_else(|| invalid(plugin, "base64 string expected"))?;
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(s)
                .map_err(|_| invalid(plugin, "invalid base64"))?;
            Ok(Value::Binary(bytes))
        }
        "json" => {
            let v = val.cloned().unwrap_or(JsonValue::Null);
            Ok(Value::Json(v))
        }
        "uuid" => {
            let s = val
                .and_then(|v| v.as_str())
                .ok_or_else(|| invalid(plugin, "uuid string expected"))?;
            let u: uuid::Uuid = s
                .parse()
                .map_err(|_| invalid(plugin, &format!("invalid uuid: {}", s)))?;
            Ok(Value::Uuid(u))
        }
        other => Err(WasmError::InvalidOutput {
            plugin: plugin.to_string(),
            reason: format!("unknown type tag: '{}'", other),
        }),
    }
}

fn json_row_to_record(json: &JsonValue, plugin: &str, index: usize) -> Result<Record, WasmError> {
    let obj = json.as_object().ok_or_else(|| WasmError::InvalidOutput {
        plugin: plugin.to_string(),
        reason: format!("record at index {} is not a JSON object", index),
    })?;

    let mut fields = Vec::with_capacity(obj.len());
    for (name, typed_val) in obj {
        let value = json_to_value(typed_val, plugin)?;
        let data_type = value.data_type();
        fields.push(FieldValue {
            name: name.clone(),
            value: if matches!(value, Value::Null) {
                None
            } else {
                Some(value)
            },
            data_type,
        });
    }

    Ok(Record::new("plugin", fields, OpType::Insert))
}

fn invalid(plugin: &str, reason: &str) -> WasmError {
    WasmError::InvalidOutput {
        plugin: plugin.to_string(),
        reason: reason.to_string(),
    }
}
