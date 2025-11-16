use crate::sql::base::{metadata::column::ColumnMetadata, query::coercion as base_coercion};
use model::core::{
    data_type::DataType,
    value::{FieldValue, Value},
};

/// Checks if a column is considered an array type.
pub(crate) fn is_array_column(col: &ColumnMetadata) -> bool {
    matches!(col.data_type, DataType::Array(_))
        || matches!(col.data_type, DataType::Set)
        || matches!(
            col.data_type,
            DataType::Custom(ref name) if name.trim().ends_with("[]")
        )
}

/// Checks if a column is considered a text-like type.
pub(crate) fn is_text_column(col: &ColumnMetadata) -> bool {
    matches!(
        col.data_type,
        DataType::String | DataType::VarChar | DataType::Char
    ) || matches!(
        col.data_type,
        DataType::Custom(ref name) if name.eq_ignore_ascii_case("text")
    )
}

/// Parses a raw string into a `Vec<String>`.
/// This attempts to handle JSON arrays, Postgres array syntax (`{}`),
/// and simple comma-separated values as fallbacks.
pub(crate) fn parse_array_string(raw: &str) -> Vec<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }

    if let Ok(json_vec) = serde_json::from_str::<Vec<String>>(trimmed) {
        return json_vec;
    }

    if trimmed.starts_with('{') && trimmed.ends_with('}') && trimmed.len() > 2 {
        let inner = &trimmed[1..trimmed.len() - 1];
        return inner
            .split(',')
            .map(|item| unescape_array_item(item.trim()))
            .filter(|item| !item.is_empty())
            .collect();
    }

    trimmed
        .split(',')
        .map(|item| item.trim())
        .filter(|item| !item.is_empty())
        .map(|item| item.trim_matches('"').trim_matches('\'').to_string())
        .collect()
}

/// Unescapes a single item from a Postgres-style array string.
pub(crate) fn unescape_array_item(raw: &str) -> String {
    let unquoted = raw.trim_matches('"');
    let mut result = String::new();
    let mut chars = unquoted.chars();
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            if let Some(next) = chars.next() {
                result.push(next);
            }
        } else {
            result.push(ch);
        }
    }
    result
}

/// Converts a `serde_json::Value` to a string.
pub(crate) fn json_value_to_string(value: &serde_json::Value) -> String {
    value
        .as_str()
        .map(|s| s.to_string())
        .unwrap_or_else(|| value.to_string())
}

/// Prepares a `Value` for `COPY` based on its column metadata.
/// This handles dispatching to array, numeric, or text coercion logic.
pub(crate) fn prepare_value(col: &ColumnMetadata, field: Option<&FieldValue>) -> Option<Value> {
    let value = field?.value.clone()?;
    Some(normalize_value(col, value))
}

/// Coerces a `Value` to a text-compatible format (e.g., `Bytes` -> `String`).
pub(crate) fn coerce_text_value(value: Value) -> Value {
    match value {
        Value::Bytes(bytes) => match String::from_utf8(bytes) {
            Ok(text) => Value::String(text),
            Err(err) => {
                let bytes = err.into_bytes();
                Value::String(String::from_utf8_lossy(&bytes).to_string())
            }
        },
        other => other,
    }
}

fn normalize_value(col: &ColumnMetadata, mut value: Value) -> Value {
    if matches!(value, Value::Null) {
        return value;
    }

    if is_array_column(col) {
        return coerce_array_value(value);
    }

    value = base_coercion::coerce_value(value, col);
    if is_text_column(col) {
        value = coerce_text_value(value);
    }

    value
}

fn coerce_array_value(value: Value) -> Value {
    match value {
        Value::StringArray(_) => value,
        Value::String(s) => Value::StringArray(parse_array_string(&s)),
        Value::Json(json) => {
            if let Some(items) = json.as_array() {
                let parsed = items.iter().map(json_value_to_string).collect::<Vec<_>>();
                Value::StringArray(parsed)
            } else {
                Value::StringArray(vec![json.to_string()])
            }
        }
        Value::Enum(_, v) => Value::StringArray(vec![v.clone()]),
        other => other,
    }
}
