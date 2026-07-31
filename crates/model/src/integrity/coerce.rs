use crate::core::value::Value;
use std::borrow::Cow;

/// Coerce a single value to match what the destination stores, before hashing.
pub fn coerce_value_for_hash<'a>(value: &'a Value, col_type: &str) -> Cow<'a, Value> {
    let col_type_lc = col_type.to_lowercase();
    if (col_type_lc.ends_with("[]") || col_type_lc.contains("array") || col_type_lc == "set")
        && let Value::String(s) = value
    {
        let elements: Vec<Value> = s
            .split(',')
            .map(|item| Value::String(item.trim_matches('"').trim_matches('\'').to_string()))
            .collect();
        return Cow::Owned(Value::Array(elements));
    }
    Cow::Borrowed(value)
}
