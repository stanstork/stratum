use crate::sql::metadata::column::ColumnMetadata;
use model::core::value::Value;

/// Coerces a value to match the target column type for PostgreSQL.
/// This handles cross-database migrations where source values might not
/// perfectly match the target column type.
pub fn coerce_value(value: Value, col: &ColumnMetadata) -> Value {
    if matches!(value, Value::Null) {
        return value;
    }

    // Check if target is an array type
    if is_array_column(col) {
        return coerce_to_array(value);
    }

    // Check if target is text type and source is binary
    if is_text_column(col) {
        return coerce_to_text(value);
    }

    value
}

/// Checks if a column is an array type.
fn is_array_column(col: &ColumnMetadata) -> bool {
    let dt = col.data_type.to_lowercase();
    dt.ends_with("[]") || dt.contains("array") || dt == "set"
}

/// Checks if a column is a text-like type.
fn is_text_column(col: &ColumnMetadata) -> bool {
    let dt = col.data_type.to_lowercase();
    dt == "text" || dt.starts_with("varchar") || dt.starts_with("char") || dt == "name"
}

/// Coerces a value to an array type.
fn coerce_to_array(value: Value) -> Value {
    match value {
        // Already array types
        Value::Array(_) | Value::Set(_) => value,

        // Parse string that might contain array data
        Value::String(s) => {
            let parsed = parse_array_string(&s);
            Value::Array(parsed.into_iter().map(Value::String).collect())
        }

        // Convert JSON array to array
        Value::Json(json) => {
            if let Some(items) = json.as_array() {
                let parsed: Vec<Value> = items
                    .iter()
                    .map(|v| {
                        if let Some(s) = v.as_str() {
                            Value::String(s.to_string())
                        } else if let Some(n) = v.as_i64() {
                            Value::Int(n)
                        } else if let Some(f) = v.as_f64() {
                            Value::Float(f)
                        } else if let Some(b) = v.as_bool() {
                            Value::Boolean(b)
                        } else if v.is_null() {
                            Value::Null
                        } else {
                            Value::String(v.to_string())
                        }
                    })
                    .collect();
                Value::Array(parsed)
            } else {
                // Single value as array
                Value::Array(vec![Value::String(json.to_string())])
            }
        }

        // Single enum value as array
        Value::Enum { value: v, .. } => Value::Array(vec![Value::String(v)]),

        // Keep other types as-is
        other => other,
    }
}

/// Coerces a value to text type.
fn coerce_to_text(value: Value) -> Value {
    match value {
        Value::Binary(bytes) => match String::from_utf8(bytes.clone()) {
            Ok(text) => Value::String(text),
            Err(_) => Value::String(String::from_utf8_lossy(&bytes).to_string()),
        },
        other => other,
    }
}

/// Parses a raw string into array elements.
/// Handles JSON arrays, PostgreSQL array syntax `{}`, and comma-separated values.
fn parse_array_string(raw: &str) -> Vec<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }

    // Try JSON array first
    if let Ok(json_vec) = serde_json::from_str::<Vec<String>>(trimmed) {
        return json_vec;
    }

    // Try PostgreSQL array syntax {a,b,c}
    if trimmed.starts_with('{') && trimmed.ends_with('}') && trimmed.len() > 2 {
        let inner = &trimmed[1..trimmed.len() - 1];
        return inner
            .split(',')
            .map(|item| unescape_array_item(item.trim()))
            .filter(|item| !item.is_empty())
            .collect();
    }

    // Fallback: comma-separated values
    trimmed
        .split(',')
        .map(|item| item.trim())
        .filter(|item| !item.is_empty())
        .map(|item| item.trim_matches('"').trim_matches('\'').to_string())
        .collect()
}

/// Unescapes a single item from a PostgreSQL-style array string.
fn unescape_array_item(raw: &str) -> String {
    let unquoted = raw.trim_matches('"');
    let mut result = String::new();
    let mut chars = unquoted.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '\\' {
            if let Some(&next) = chars.peek() {
                chars.next();
                result.push(next);
            }
        } else {
            result.push(ch);
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_pg_array() {
        let result = parse_array_string("{a,b,c}");
        assert_eq!(result, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_parse_json_array() {
        let result = parse_array_string(r#"["a","b","c"]"#);
        assert_eq!(result, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_parse_csv() {
        let result = parse_array_string("a,b,c");
        assert_eq!(result, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_unescape_quoted() {
        let result = unescape_array_item(r#""hello\"world""#);
        assert_eq!(result, r#"hello"world"#);
    }
}
