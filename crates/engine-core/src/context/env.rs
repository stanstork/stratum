use std::collections::HashMap;
use std::sync::RwLock;

/// Global environment variable context for runtime evaluation
static ENV_CONTEXT: RwLock<Option<EnvContext>> = RwLock::new(None);

#[derive(Debug, Clone)]
pub struct EnvContext {
    vars: HashMap<String, String>,
}

impl EnvContext {
    pub fn new() -> Self {
        let mut vars = HashMap::new();

        // Load all system environment variables
        for (key, value) in std::env::vars() {
            vars.insert(key, value);
        }

        Self { vars }
    }

    pub fn empty() -> Self {
        Self {
            vars: HashMap::new(),
        }
    }

    pub fn get(&self, key: &str) -> Option<String> {
        self.vars.get(key).cloned()
    }

    pub fn get_or(&self, key: &str, default: &str) -> String {
        self.vars
            .get(key)
            .cloned()
            .unwrap_or_else(|| default.to_string())
    }

    pub fn set(&mut self, key: String, value: String) {
        self.vars.insert(key, value);
    }

    pub fn merge(&mut self, vars: HashMap<String, String>) {
        self.vars.extend(vars);
    }
}

impl Default for EnvContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Initialize the global environment context
pub fn init_env_context(context: EnvContext) {
    let mut guard = ENV_CONTEXT.write().unwrap();
    *guard = Some(context);
}

/// Get a value from the global environment context
pub fn get_env(key: &str) -> Option<String> {
    let guard = ENV_CONTEXT.read().unwrap();
    guard.as_ref().and_then(|ctx| ctx.get(key))
}

/// Get a value from the global environment context with a default
pub fn get_env_or(key: &str, default: &str) -> String {
    let guard = ENV_CONTEXT.read().unwrap();
    guard
        .as_ref()
        .map(|ctx| ctx.get_or(key, default))
        .unwrap_or_else(|| default.to_string())
}

#[cfg(any(test, debug_assertions))]
pub fn clear_env_context() {
    let mut guard = ENV_CONTEXT.write().unwrap();
    *guard = None;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_env_context() {
        let mut ctx = EnvContext::empty();
        ctx.set("TEST_KEY".to_string(), "test_value".to_string());

        assert_eq!(ctx.get("TEST_KEY"), Some("test_value".to_string()));
        assert_eq!(ctx.get("MISSING"), None);
        assert_eq!(ctx.get_or("MISSING", "default"), "default");
    }

    #[test]
    fn test_global_context() {
        clear_env_context();

        let mut ctx = EnvContext::empty();
        ctx.set("GLOBAL_TEST".to_string(), "value".to_string());
        init_env_context(ctx);

        assert_eq!(get_env("GLOBAL_TEST"), Some("value".to_string()));
        assert_eq!(get_env_or("GLOBAL_TEST", "default"), "value");
        assert_eq!(get_env_or("MISSING", "default"), "default");

        clear_env_context();
    }
}
