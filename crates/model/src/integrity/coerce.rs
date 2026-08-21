use crate::core::value::Value;
use std::borrow::Cow;

/// Whether a destination column stores an array-like value.
pub fn is_array_like(col_type: &str) -> bool {
    col_type.ends_with("[]")
        || col_type.eq_ignore_ascii_case("set")
        || col_type
            .as_bytes()
            .windows(5)
            .any(|w| w.eq_ignore_ascii_case(b"array"))
}

/// Coerce a value bound for an array-like column, so the hash matches what the
/// destination actually stores.
pub fn coerce_array_value(value: &Value) -> Cow<'_, Value> {
    let Value::String(s) = value else {
        return Cow::Borrowed(value);
    };

    let elements = s
        .split(',')
        .map(|item| Value::String(item.trim_matches(&['"', '\''][..]).to_string()))
        .collect();

    Cow::Owned(Value::Array(elements))
}
