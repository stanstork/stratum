use crate::{context::EvalContext, error::Result};
use model::core::value::Value;

/// Return the first non-null argument, or `Null` if every argument is null.
pub fn eval_coalesce(args: &[Value], _ctx: &EvalContext) -> Result<Value> {
    Ok(args
        .iter()
        .find(|v| !matches!(v, Value::Null))
        .cloned()
        .unwrap_or(Value::Null))
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
    fn test_coalesce_first_non_null() {
        with_dummy_ctx(|ctx| {
            let args = vec![
                Value::Null,
                Value::String("fallback".to_string()),
                Value::String("ignored".to_string()),
            ];
            let result = eval_coalesce(&args, ctx).unwrap();
            assert_eq!(result, Value::String("fallback".to_string()));
        });
    }

    #[test]
    fn test_coalesce_all_null() {
        with_dummy_ctx(|ctx| {
            let args = vec![Value::Null, Value::Null];
            assert_eq!(eval_coalesce(&args, ctx).unwrap(), Value::Null);
        });
    }

    #[test]
    fn test_coalesce_empty() {
        with_dummy_ctx(|ctx| {
            assert_eq!(eval_coalesce(&[], ctx).unwrap(), Value::Null);
        });
    }
}
