use crate::{
    context::EvalContext,
    error::{ExpressionError, Result},
    types::parse_env_as_type,
};
use model::core::value::Value;

/// Evaluate env() function with type-aware default handling
///
/// Syntax:
/// - env("VAR_NAME") - Required variable, fails if missing
/// - env("VAR_NAME", default) - Optional variable, returns typed default if missing
pub fn eval_env(args: &[Value], ctx: &EvalContext) -> Result<Value> {
    match args.len() {
        1 => {
            // Required environment variable
            let var_name = match args.first() {
                Some(Value::String(s)) => s.as_str(),
                _ => {
                    return Err(ExpressionError::InvalidFunctionArgs {
                        function: "env".to_string(),
                        message: "First argument must be a string (variable name)".to_string(),
                    });
                }
            };

            ctx.get_env(var_name)
                .map(Value::String)
                .ok_or_else(|| ExpressionError::MissingRequiredEnvVar(var_name.to_string()))
        }
        2 => {
            // Optional environment variable with default
            let var_name = match args.first() {
                Some(Value::String(s)) => s.as_str(),
                _ => {
                    return Err(ExpressionError::InvalidFunctionArgs {
                        function: "env".to_string(),
                        message: "First argument must be a string (variable name)".to_string(),
                    });
                }
            };

            let default_value = args.get(1).unwrap();

            // If env var exists, try to parse it as the type of the default value
            if let Some(env_str) = ctx.get_env(var_name) {
                parse_env_as_type(&env_str, default_value).ok_or_else(|| {
                    ExpressionError::EnvParseError {
                        var: var_name.to_string(),
                        value: env_str,
                        expected_type: format!("{:?}", default_value),
                    }
                })
            } else {
                // Return default value with its original type
                Ok(default_value.clone())
            }
        }
        _ => Err(ExpressionError::InvalidFunctionArgs {
            function: "env".to_string(),
            message: format!("Expected 1 or 2 arguments, got {}", args.len()),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use engine_core::context::env::EnvContext;
    use std::collections::HashMap;

    #[test]
    fn test_env_required_exists() {
        let mut env = EnvContext::empty();
        env.set("TEST_VAR".to_string(), "test_value".to_string());
        let env_getter = |key: &str| env.get(key);

        let definitions = HashMap::new();
        let ctx = EvalContext::BuildTime {
            definitions: &definitions,
            env_getter: &env_getter,
        };

        let args = vec![Value::String("TEST_VAR".to_string())];
        let result = eval_env(&args, &ctx).unwrap();
        assert_eq!(result, Value::String("test_value".to_string()));
    }

    #[test]
    fn test_env_required_missing() {
        let env = EnvContext::empty();
        let env_getter = |key: &str| env.get(key);

        let definitions = HashMap::new();
        let ctx = EvalContext::BuildTime {
            definitions: &definitions,
            env_getter: &env_getter,
        };

        let args = vec![Value::String("MISSING_VAR".to_string())];
        let result = eval_env(&args, &ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_env_with_typed_default_int() {
        let mut env = EnvContext::empty();
        env.set("BATCH_SIZE".to_string(), "5000".to_string());
        let env_getter = |key: &str| env.get(key);

        let definitions = HashMap::new();
        let ctx = EvalContext::BuildTime {
            definitions: &definitions,
            env_getter: &env_getter,
        };

        let args = vec![
            Value::String("BATCH_SIZE".to_string()),
            Value::Float(1000.0),
        ];
        let result = eval_env(&args, &ctx).unwrap();
        assert_eq!(result, Value::UInt(5000));
    }

    #[test]
    fn test_env_with_typed_default_bool() {
        let mut env = EnvContext::empty();
        env.set("ENABLE_FEATURE".to_string(), "true".to_string());
        let env_getter = |key: &str| env.get(key);

        let definitions = HashMap::new();
        let ctx = EvalContext::BuildTime {
            definitions: &definitions,
            env_getter: &env_getter,
        };

        let args = vec![
            Value::String("ENABLE_FEATURE".to_string()),
            Value::Boolean(false),
        ];
        let result = eval_env(&args, &ctx).unwrap();
        assert_eq!(result, Value::Boolean(true));
    }

    #[test]
    fn test_env_missing_returns_default() {
        let env = EnvContext::empty();
        let env_getter = |key: &str| env.get(key);

        let definitions = HashMap::new();
        let ctx = EvalContext::BuildTime {
            definitions: &definitions,
            env_getter: &env_getter,
        };

        let args = vec![Value::String("MISSING".to_string()), Value::Float(1234.0)];
        let result = eval_env(&args, &ctx).unwrap();
        assert_eq!(result, Value::Float(1234.0));
    }

    #[test]
    fn test_env_parse_failure() {
        let mut env = EnvContext::empty();
        env.set("BAD_INT".to_string(), "not_a_number".to_string());
        let env_getter = |key: &str| env.get(key);

        let definitions = HashMap::new();
        let ctx = EvalContext::BuildTime {
            definitions: &definitions,
            env_getter: &env_getter,
        };

        let args = vec![Value::String("BAD_INT".to_string()), Value::Float(100.0)];
        let result = eval_env(&args, &ctx);
        assert!(result.is_err());
    }
}
