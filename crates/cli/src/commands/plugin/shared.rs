use crate::error::CliError;
use engine_wasm::{
    registry::resolve_limits, runtime::limits::ResourceLimits, schema::PluginMetadata,
};
use model::{core::value::Value, execution::plugin::PluginDecl};
use std::io::Read;

pub use engine_wasm::registry::caps_from_decl;

/// Limits to run a plugin with: start from the runtime/role-appropriate ceiling
/// (`suggested_limits`), then let any explicit SMQL override win. Shares the
/// runtime's `resolve_limits` so CLI tooling and `apply` size plugins identically.
pub fn limits_for(meta: &PluginMetadata, decl: Option<&PluginDecl>) -> ResourceLimits {
    match decl {
        Some(d) => resolve_limits(meta, d),
        None => meta.suggested_limits(),
    }
}

/// Convert a scalar JSON value into a model `Value` (plain author-facing JSON,
/// not the `{type,value}` wire envelope - the exchange layer adds that).
pub fn json_to_value(v: &serde_json::Value) -> Value {
    match v {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Boolean(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else if let Some(u) = n.as_u64() {
                Value::UInt(u)
            } else {
                Value::Float(n.as_f64().unwrap_or(0.0))
            }
        }
        serde_json::Value::String(s) => Value::String(s.clone()),
        other => Value::Json(other.clone()),
    }
}

pub fn read_input(src: Option<&str>) -> Result<String, CliError> {
    if let Some(s) = src
        && matches!(s.trim_start().chars().next(), Some('{' | '['))
    {
        return Ok(s.to_string());
    }
    read_text(src)
}

/// Read text from a file path, or stdin when the source is `"-"`. `None` -> "".
pub fn read_text(src: Option<&str>) -> Result<String, CliError> {
    match src {
        None => Ok(String::new()),
        Some("-") => {
            let mut s = String::new();
            std::io::stdin()
                .read_to_string(&mut s)
                .map_err(|e| CliError::UserMessage(format!("reading stdin: {e}")))?;
            Ok(s)
        }
        Some(path) => std::fs::read_to_string(path)
            .map_err(|e| CliError::UserMessage(format!("reading {path}: {e}"))),
    }
}
