pub mod abi;
pub mod pack;
pub mod panic;

use crate::error::{PluginError, PluginResult};
use std::collections::HashMap;

/// Parse the cursor wire form `{"cursor": "abc"}` (or `{"cursor": null}`, or `{}`)
/// into an optional cursor string.
#[doc(hidden)]
pub fn parse_cursor(bytes: &[u8]) -> PluginResult<Option<String>> {
    if bytes.is_empty() {
        return Ok(None);
    }
    let parsed: serde_json::Value = serde_json::from_slice(bytes)?;
    Ok(parsed
        .get("cursor")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string()))
}

/// Parse a JSON config object delivered to `__stratum_initialize` into a
/// `HashMap<String, String>`. Non-string values are coerced via `to_string()`.
#[doc(hidden)]
pub fn parse_config(bytes: &[u8]) -> PluginResult<HashMap<String, String>> {
    if bytes.is_empty() {
        return Ok(HashMap::new());
    }
    let json: serde_json::Value = serde_json::from_slice(bytes)?;
    let obj = json
        .as_object()
        .ok_or_else(|| PluginError::invalid_input("config must be a JSON object"))?;
    let mut out = HashMap::with_capacity(obj.len());
    for (k, v) in obj {
        let s = match v {
            serde_json::Value::String(s) => s.clone(),
            serde_json::Value::Null => continue,
            other => other.to_string(),
        };
        out.insert(k.clone(), s);
    }
    Ok(out)
}
