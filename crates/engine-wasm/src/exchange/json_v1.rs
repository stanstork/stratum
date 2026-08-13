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

/// Coerce a value toward the plugin's declared input type tag.
pub(crate) fn coerce_value(value: &Value, tag: &str) -> Value {
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

/// Serialize a whole batch of transform inputs as a JSON array of objects - one
/// per row, in order. The guest processes them all in a single call, amortizing
/// the WASM boundary crossing (alloc/serialize/call/deserialize) over the batch.
pub fn serialize_input_from_records(
    rows: &[Record],
    schema: &[PluginField],
    mapping: &HashMap<String, String>,
) -> Result<Vec<u8>, WasmError> {
    let src_schema = rows.first().map(|r| r.schema());
    let plan: Vec<(&str, Option<usize>, &str)> = schema
        .iter()
        .map(|f| {
            let idx = mapping
                .get(&f.name)
                .and_then(|src| src_schema.and_then(|s| s.index_of(src)));
            (f.name.as_str(), idx, f.field_type.as_str())
        })
        .collect();

    let arr: Vec<JsonValue> = rows
        .iter()
        .map(|row| {
            let mut map = Map::new();
            for (name, idx, tag) in &plan {
                let json = idx.and_then(|i| row.value_at(i)).map_or_else(
                    || value_to_raw_json(&Value::Null),
                    |v| value_to_raw_json(&coerce_value(v, tag)),
                );
                map.insert(name.to_string(), json);
            }
            JsonValue::Object(map)
        })
        .collect();

    serde_json::to_vec(&JsonValue::Array(arr))
        .map_err(|e| WasmError::SerializationError(e.to_string()))
}

pub fn serialize_input_batch(
    inputs: &[PluginInput],
    schema: &[PluginField],
) -> Result<Vec<u8>, WasmError> {
    let declared: HashMap<&str, &str> = schema
        .iter()
        .map(|f| (f.name.as_str(), f.field_type.as_str()))
        .collect();

    let rows: Vec<JsonValue> = inputs
        .iter()
        .map(|input| {
            let mut map = Map::new();
            for (key, value) in input.fields() {
                let json = declared.get(key.as_str()).map_or_else(
                    || value_to_raw_json(value),
                    |&tag| value_to_raw_json(&coerce_value(value, tag)),
                );
                map.insert(key.clone(), json);
            }
            JsonValue::Object(map)
        })
        .collect();

    serde_json::to_vec(&JsonValue::Array(rows))
        .map_err(|e| WasmError::SerializationError(e.to_string()))
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
            for field in record.iter() {
                let value = match field.value {
                    Some(v) => value_to_json(v),
                    None => value_to_json(&Value::Null),
                };
                row.insert(field.name.to_string(), value);
            }
            JsonValue::Object(row)
        })
        .collect();

    serde_json::to_vec(&json!({ "records": records }))
        .map_err(|e| WasmError::SerializationError(e.to_string()))
}

/// Deserialize a batch of transform outputs: a JSON array of output values (one
/// per input, in order), or a guest-side `{"error": ...}` object.
pub fn deserialize_output_batch(
    bytes: &[u8],
    plugin: &str,
) -> Result<Vec<PluginOutput>, WasmError> {
    let json: JsonValue = serde_json::from_slice(bytes)
        .map_err(|e| WasmError::DeserializationError(format!("{}: {}", plugin, e)))?;

    check_guest_error(&json, plugin)?;

    match json {
        JsonValue::Array(items) => items
            .iter()
            .map(|item| {
                Ok(PluginOutput {
                    value: json_to_value(item, plugin)?,
                })
            })
            .collect(),
        other => Err(WasmError::DeserializationError(format!(
            "{}: transform_batch expected an array of outputs, got {other}",
            plugin
        ))),
    }
}

/// Deserialize a batch of filter decisions: a JSON array of `{"pass": ...}`
/// objects (one per input, in order), or a guest-side `{"error": ...}` object.
pub fn deserialize_filter_decision_batch(
    bytes: &[u8],
    plugin: &str,
) -> Result<Vec<FilterDecision>, WasmError> {
    let json: JsonValue = serde_json::from_slice(bytes)
        .map_err(|e| WasmError::DeserializationError(format!("{}: {}", plugin, e)))?;

    if let Some(err) = json.get("error") {
        return Err(WasmError::PluginError {
            plugin: plugin.to_string(),
            message: err.as_str().unwrap_or("unknown error").to_string(),
        });
    }

    match json {
        JsonValue::Array(items) => items
            .iter()
            .map(|item| decode_decision(item, plugin))
            .collect(),
        other => Err(WasmError::DeserializationError(format!(
            "{}: evaluate expected an array of decisions, got {other}",
            plugin
        ))),
    }
}

pub fn deserialize_source_page(bytes: &[u8], plugin: &str) -> Result<SourcePage, WasmError> {
    let json: JsonValue = serde_json::from_slice(bytes)
        .map_err(|e| WasmError::DeserializationError(format!("{}: {}", plugin, e)))?;

    check_guest_error(&json, plugin)?;

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

    check_guest_error(&json, plugin)?;

    let rows_written = json
        .get("rows_written")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| WasmError::InvalidOutput {
            plugin: plugin.to_string(),
            reason: "missing 'rows_written' integer".to_string(),
        })?;

    Ok(WriteResult { rows_written })
}

pub(crate) fn value_to_json(value: &Value) -> JsonValue {
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

/// The *bare* JSON value with no `{type, value}` envelope.
pub(crate) fn value_to_raw_json(value: &Value) -> JsonValue {
    match value {
        Value::Null => JsonValue::Null,
        Value::Boolean(b) => json!(b),
        Value::Int(i) => json!(i),
        Value::UInt(u) => json!(u),
        Value::Float(f) => json!(f),
        Value::Decimal(d) => json!(d.to_string()),
        Value::String(s) => json!(s),
        Value::Binary(b) => {
            use base64::Engine;
            json!(base64::engine::general_purpose::STANDARD.encode(b))
        }
        Value::Date(d) => json!(d.to_string()),
        Value::Timestamp {
            value: ts,
            offset_secs,
        } => {
            let s = if let Some(offset) = offset_secs {
                format!("{}+{:02}:{:02}", ts, offset / 3600, (offset % 3600) / 60)
            } else {
                format!("{}Z", ts)
            };
            json!(s)
        }
        Value::Time { value: t, .. } => json!(t.to_string()),
        Value::Uuid(u) => json!(u.to_string()),
        Value::Json(j) => j.clone(),
        Value::Enum { value: v, .. } => json!(v),
        other => json!(format!("{:?}", other)),
    }
}

/// Reconstruct a `Value` from a bare JSON value using the plugin's *declared*
/// output type (the transform's `output = "..."`).
pub(crate) fn raw_json_to_value(json: &JsonValue, output_type: Option<&str>) -> Value {
    if json.is_null() {
        return Value::Null;
    }

    let ty = output_type.map(|t| t.to_ascii_lowercase());
    match ty.as_deref() {
        Some("f64" | "float" | "double" | "real") => {
            json.as_f64().map(Value::Float).unwrap_or(Value::Null)
        }
        Some("i64" | "int" | "integer" | "bigint" | "smallint" | "tinyint") => json
            .as_i64()
            .map(Value::Int)
            .or_else(|| json.as_f64().map(|f| Value::Int(f as i64)))
            .unwrap_or(Value::Null),
        Some("u64" | "uint" | "unsigned") => json.as_u64().map(Value::UInt).unwrap_or(Value::Null),
        Some("bool" | "boolean") => json.as_bool().map(Value::Boolean).unwrap_or(Value::Null),
        Some("decimal" | "numeric") => match json {
            JsonValue::String(s) => s.parse().ok().map(Value::Decimal).unwrap_or(Value::Null),
            _ => json
                .as_f64()
                .and_then(|f| bigdecimal::BigDecimal::try_from(f).ok())
                .map(Value::Decimal)
                .unwrap_or(Value::Null),
        },
        Some("json") => Value::Json(json.clone()),
        Some("string" | "text" | "varchar" | "char") => match json {
            JsonValue::String(s) => Value::String(s.clone()),
            other => Value::String(other.to_string()),
        },
        _ => match json {
            JsonValue::Bool(b) => Value::Boolean(*b),
            JsonValue::Number(n) if n.is_i64() => Value::Int(n.as_i64().unwrap_or(0)),
            JsonValue::Number(n) if n.is_u64() => Value::UInt(n.as_u64().unwrap_or(0)),
            JsonValue::Number(n) => Value::Float(n.as_f64().unwrap_or(0.0)),
            JsonValue::String(s) => Value::String(s.clone()),
            other => Value::Json(other.clone()),
        },
    }
}

/// Deserialize a flat (unenveloped) transform-output array of raw values.
pub fn deserialize_output_flat(
    bytes: &[u8],
    plugin: &str,
    output_type: Option<&str>,
) -> Result<Vec<PluginOutput>, WasmError> {
    let json: JsonValue = serde_json::from_slice(bytes)
        .map_err(|e| WasmError::DeserializationError(format!("{}: {}", plugin, e)))?;

    check_guest_error(&json, plugin)?;

    match json {
        JsonValue::Array(items) => Ok(items
            .iter()
            .map(|item| PluginOutput {
                value: raw_json_to_value(item, output_type),
            })
            .collect()),
        other => Err(WasmError::DeserializationError(format!(
            "{}: transform expected an array of outputs, got {other}",
            plugin
        ))),
    }
}

pub(crate) fn json_to_value(json: &JsonValue, plugin: &str) -> Result<Value, WasmError> {
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

    Ok(Record::from_fields("plugin", fields, OpType::Insert))
}

fn decode_decision(json: &JsonValue, plugin: &str) -> Result<FilterDecision, WasmError> {
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

fn invalid(plugin: &str, reason: &str) -> WasmError {
    WasmError::InvalidOutput {
        plugin: plugin.to_string(),
        reason: reason.to_string(),
    }
}

fn check_guest_error(json: &JsonValue, plugin: &str) -> Result<(), WasmError> {
    if let Some(err) = json.get("error") {
        Err(WasmError::PluginError {
            plugin: plugin.to_string(),
            message: err.as_str().unwrap_or("unknown error").to_string(),
        })
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn input_batch_is_json_array_of_objects() {
        let mut a = PluginInput::new();
        a.insert("x".to_string(), Value::Int(1));
        let mut b = PluginInput::new();
        b.insert("x".to_string(), Value::Int(2));

        let bytes = serialize_input_batch(&[a, b], &[]).unwrap();
        let json: JsonValue = serde_json::from_slice(&bytes).unwrap();
        let arr = json.as_array().expect("batch input is a JSON array");
        assert_eq!(arr.len(), 2);
        assert!(arr.iter().all(|e| e.is_object()));
    }

    #[test]
    fn output_batch_roundtrips_values() {
        let arr = JsonValue::Array(vec![
            value_to_json(&Value::Int(10)),
            value_to_json(&Value::String("hi".to_string())),
        ]);
        let bytes = serde_json::to_vec(&arr).unwrap();

        let outs = deserialize_output_batch(&bytes, "p").unwrap();
        assert_eq!(outs.len(), 2);
        assert_eq!(outs[0].value, Value::Int(10));
        assert_eq!(outs[1].value, Value::String("hi".to_string()));
    }

    #[test]
    fn output_batch_surfaces_guest_error() {
        let err = serde_json::to_vec(&json!({ "error": "boom" })).unwrap();
        assert!(matches!(
            deserialize_output_batch(&err, "p"),
            Err(WasmError::PluginError { .. })
        ));
    }
}
