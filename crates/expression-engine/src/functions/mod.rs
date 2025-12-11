pub mod env;
pub mod string;

use crate::{
    context::EvalContext,
    error::{ExpressionError, Result},
};
use model::core::value::Value;
use std::collections::HashMap;

/// Type alias for function implementations
pub type FunctionImpl = fn(&[Value], &EvalContext) -> Result<Value>;

/// Registry of all available functions
pub struct FunctionRegistry {
    functions: HashMap<String, FunctionImpl>,
}

impl FunctionRegistry {
    /// Create a new function registry with all built-in functions
    pub fn new() -> Self {
        let mut registry = Self {
            functions: HashMap::new(),
        };

        // Register built-in functions
        registry.register("env", env::eval_env);
        registry.register("lower", string::eval_lower);
        registry.register("upper", string::eval_upper);
        registry.register("concat", string::eval_concat);

        registry
    }

    pub fn register(&mut self, name: &str, func: FunctionImpl) {
        self.functions.insert(name.to_lowercase(), func);
    }

    pub fn call(&self, name: &str, args: &[Value], ctx: &EvalContext) -> Result<Value> {
        let func = self
            .functions
            .get(&name.to_lowercase())
            .ok_or_else(|| ExpressionError::UnknownFunction(name.to_string()))?;

        func(args, ctx)
    }

    pub fn has_function(&self, name: &str) -> bool {
        self.functions.contains_key(&name.to_lowercase())
    }

    pub fn function_names(&self) -> Vec<&str> {
        self.functions.keys().map(|s| s.as_str()).collect()
    }
}

impl Default for FunctionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap as StdHashMap;

    fn dummy_env_getter(_key: &str) -> Option<String> {
        None
    }

    #[test]
    fn test_registry_has_builtin_functions() {
        let registry = FunctionRegistry::new();
        assert!(registry.has_function("env"));
        assert!(registry.has_function("lower"));
        assert!(registry.has_function("upper"));
        assert!(registry.has_function("concat"));
    }

    #[test]
    fn test_registry_case_insensitive() {
        let registry = FunctionRegistry::new();
        assert!(registry.has_function("ENV"));
        assert!(registry.has_function("Lower"));
        assert!(registry.has_function("UPPER"));
    }

    #[test]
    fn test_call_function() {
        let registry = FunctionRegistry::new();
        let definitions = StdHashMap::new();
        let ctx = EvalContext::BuildTime {
            definitions: &definitions,
            env_getter: dummy_env_getter,
        };

        let args = vec![Value::String("hello".to_string())];
        let result = registry.call("upper", &args, &ctx).unwrap();
        assert_eq!(result, Value::String("HELLO".to_string()));
    }

    #[test]
    fn test_unknown_function() {
        let registry = FunctionRegistry::new();
        let definitions = StdHashMap::new();
        let ctx = EvalContext::BuildTime {
            definitions: &definitions,
            env_getter: dummy_env_getter,
        };

        let result = registry.call("unknown_func", &[], &ctx);
        assert!(result.is_err());
        assert!(matches!(result, Err(ExpressionError::UnknownFunction(_))));
    }

    #[test]
    fn test_custom_function_registration() {
        let mut registry = FunctionRegistry::new();

        fn custom_func(args: &[Value], _ctx: &EvalContext) -> Result<Value> {
            Ok(Value::String(format!("custom: {:?}", args)))
        }

        registry.register("custom", custom_func);
        assert!(registry.has_function("custom"));

        let definitions = StdHashMap::new();
        let ctx = EvalContext::BuildTime {
            definitions: &definitions,
            env_getter: dummy_env_getter,
        };
        let result = registry.call("custom", &[], &ctx);
        assert!(result.is_ok());
    }
}
