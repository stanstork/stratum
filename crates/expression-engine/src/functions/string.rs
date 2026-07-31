use crate::{
    context::EvalContext,
    error::{ExpressionError, Result},
};
use model::core::value::Value;

/// Convert string to lowercase
pub fn eval_lower(args: &[Value], _ctx: &EvalContext) -> Result<Value> {
    match args.first() {
        Some(Value::String(s)) => Ok(Value::String(s.to_lowercase())),
        Some(other) => Err(ExpressionError::InvalidFunctionArgs {
            function: "lower".to_string(),
            message: format!("Expected string, got {:?}", other),
        }),
        None => Err(ExpressionError::InvalidFunctionArgs {
            function: "lower".to_string(),
            message: "Expected 1 argument, got 0".to_string(),
        }),
    }
}

/// Convert string to uppercase
pub fn eval_upper(args: &[Value], _ctx: &EvalContext) -> Result<Value> {
    match args.first() {
        Some(Value::String(s)) => Ok(Value::String(s.to_uppercase())),
        Some(other) => Err(ExpressionError::InvalidFunctionArgs {
            function: "upper".to_string(),
            message: format!("Expected string, got {:?}", other),
        }),
        None => Err(ExpressionError::InvalidFunctionArgs {
            function: "upper".to_string(),
            message: "Expected 1 argument, got 0".to_string(),
        }),
    }
}

/// Strip leading and trailing whitespace from a string
pub fn eval_trim(args: &[Value], _ctx: &EvalContext) -> Result<Value> {
    match args.first() {
        Some(Value::String(s)) => Ok(Value::String(s.trim().to_string())),
        Some(Value::Null) | None => Ok(Value::Null),
        Some(other) => Err(ExpressionError::InvalidFunctionArgs {
            function: "trim".to_string(),
            message: format!("Expected string, got {:?}", other),
        }),
    }
}

/// Concatenate multiple values into a string
pub fn eval_concat(args: &[Value], _ctx: &EvalContext) -> Result<Value> {
    let mut out = String::new();
    for arg in args {
        write_value_string(arg, &mut out);
    }
    Ok(Value::String(out))
}

fn write_value_string(value: &Value, out: &mut String) {
    use std::fmt::Write as _;
    match value {
        Value::String(s) => out.push_str(s),
        Value::Int(i) => {
            let _ = write!(out, "{i}");
        }
        Value::UInt(u) => {
            let _ = write!(out, "{u}");
        }
        Value::Float(f) => {
            let _ = write!(out, "{f}");
        }
        Value::Decimal(d) => {
            let _ = write!(out, "{d}");
        }
        Value::Boolean(b) => {
            let _ = write!(out, "{b}");
        }
        Value::Null => {}
        other => {
            let _ = write!(out, "{other:?}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn dummy_env_getter(_key: &str) -> Option<String> {
        None
    }

    fn with_dummy_ctx<F, R>(f: F) -> R
    where
        F: FnOnce(&EvalContext) -> R,
    {
        let definitions = HashMap::new();
        let ctx = EvalContext::BuildTime {
            definitions: &definitions,
            env_getter: &dummy_env_getter,
        };
        f(&ctx)
    }

    #[test]
    fn test_lower() {
        with_dummy_ctx(|ctx| {
            let args = vec![Value::String("HELLO".to_string())];
            let result = eval_lower(&args, ctx).unwrap();
            assert_eq!(result, Value::String("hello".to_string()));
        });
    }

    #[test]
    fn test_upper() {
        with_dummy_ctx(|ctx| {
            let args = vec![Value::String("world".to_string())];
            let result = eval_upper(&args, ctx).unwrap();
            assert_eq!(result, Value::String("WORLD".to_string()));
        });
    }

    #[test]
    fn test_concat() {
        with_dummy_ctx(|ctx| {
            let args = vec![
                Value::String("Hello".to_string()),
                Value::String(" ".to_string()),
                Value::String("World".to_string()),
            ];
            let result = eval_concat(&args, ctx).unwrap();
            assert_eq!(result, Value::String("Hello World".to_string()));
        });
    }

    #[test]
    fn test_concat_mixed_types() {
        with_dummy_ctx(|ctx| {
            let args = vec![Value::String("Count: ".to_string()), Value::Int(42)];
            let result = eval_concat(&args, ctx).unwrap();
            assert_eq!(result, Value::String("Count: 42".to_string()));
        });
    }
}
