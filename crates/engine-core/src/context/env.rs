use std::collections::HashMap;

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
}
