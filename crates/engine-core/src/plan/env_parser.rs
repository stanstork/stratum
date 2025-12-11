use model::core::value::Value;

/// Parse an environment variable string as the type of the given default value
pub fn parse_env_as_type(env_str: &str, default_value: &Value) -> Option<Value> {
    match default_value {
        Value::String(_) => Some(Value::String(env_str.to_string())),
        Value::Boolean(_) => env_str
            .to_lowercase()
            .parse::<bool>()
            .ok()
            .map(Value::Boolean),
        Value::Int(i) if *i >= 0 => env_str.parse::<u64>().ok().map(Value::Uint),
        Value::Int(_) => env_str.parse::<i64>().ok().map(Value::Int),
        Value::Uint(_) => env_str.parse::<u64>().ok().map(Value::Uint),
        Value::Int32(_) => env_str.parse::<i32>().ok().map(Value::Int32),
        Value::SmallInt(_) => env_str.parse::<i16>().ok().map(Value::SmallInt),
        Value::Usize(_) => env_str.parse::<usize>().ok().map(Value::Usize),
        Value::Float(f) => {
            // If the default is a whole number and the env var looks like an integer, parse as integer
            if f.fract() == 0.0 && !env_str.contains('.') {
                // Try to parse as integer first (for whole numbers)
                if let Ok(u) = env_str.parse::<u64>() {
                    return Some(Value::Uint(u));
                }
                if let Ok(i) = env_str.parse::<i64>() {
                    return Some(Value::Int(i));
                }
            }
            // Parse as float - will fail if not a valid number
            env_str.parse::<f64>().ok().map(Value::Float)
        }
        // For other types, just return as string (no validation needed)
        _ => Some(Value::String(env_str.to_string())),
    }
}
