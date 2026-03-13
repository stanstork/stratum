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
        Value::Int(i) if *i >= 0 => env_str.parse::<u64>().ok().map(Value::UInt),
        Value::Int(_) => env_str.parse::<i64>().ok().map(Value::Int),
        Value::UInt(_) => env_str.parse::<u64>().ok().map(Value::UInt),
        Value::Float(f) => {
            // If the default is a whole number and the env var looks like an integer, parse as integer
            if f.fract() == 0.0 && !env_str.contains('.') {
                // Try to parse as integer first (for whole numbers)
                if let Ok(u) = env_str.parse::<u64>() {
                    return Some(Value::UInt(u));
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_env_as_bool() {
        let default = Value::Boolean(false);
        assert_eq!(
            parse_env_as_type("true", &default),
            Some(Value::Boolean(true))
        );
        assert_eq!(
            parse_env_as_type("false", &default),
            Some(Value::Boolean(false))
        );
        assert_eq!(parse_env_as_type("invalid", &default), None);
    }

    #[test]
    fn test_parse_env_as_int() {
        let default = Value::Int(100);
        assert_eq!(parse_env_as_type("42", &default), Some(Value::UInt(42)));
        assert_eq!(parse_env_as_type("not_a_number", &default), None);
    }

    #[test]
    fn test_parse_env_as_float_with_decimal() {
        let default = Value::Float(std::f64::consts::PI);
        assert_eq!(
            parse_env_as_type("2.71", &default),
            Some(Value::Float(2.71))
        );
    }

    #[test]
    fn test_parse_env_as_float_whole_number() {
        let default = Value::Float(100.0);
        // Should parse as integer when env var doesn't have decimal point
        assert_eq!(parse_env_as_type("5000", &default), Some(Value::UInt(5000)));
        // Should parse as float when env var has decimal point
        assert_eq!(
            parse_env_as_type("5000.0", &default),
            Some(Value::Float(5000.0))
        );
    }

    #[test]
    fn test_parse_env_as_string() {
        let default = Value::String("default".to_string());
        assert_eq!(
            parse_env_as_type("any value", &default),
            Some(Value::String("any value".to_string()))
        );
    }
}
