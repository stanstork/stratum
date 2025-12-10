use std::collections::HashMap;
use std::fs;
use std::path::Path;

use crate::error::CliError;

/// Environment variable manager that loads from system and .env files
#[derive(Debug, Clone)]
pub struct EnvManager {
    vars: HashMap<String, String>,
    _sensitive_patterns: Vec<String>,
}

impl EnvManager {
    pub fn new() -> Self {
        let mut vars = HashMap::new();

        // Load all system environment variables
        for (key, value) in std::env::vars() {
            vars.insert(key, value);
        }

        Self {
            vars,
            _sensitive_patterns: Self::default_sensitive_patterns(),
        }
    }

    /// Load variables from a .env file
    pub fn load_from_file<P: AsRef<Path>>(&mut self, path: P) -> Result<(), CliError> {
        let path = path.as_ref();
        let content = fs::read_to_string(path).map_err(|e| {
            CliError::Config(format!("Failed to read env file {}: {}", path.display(), e))
        })?;

        self.parse_env_content(&content)?;
        Ok(())
    }

    pub fn all(&self) -> &HashMap<String, String> {
        &self.vars
    }

    fn parse_env_content(&mut self, content: &str) -> Result<(), CliError> {
        for (line_num, line) in content.lines().enumerate() {
            let line = line.trim();

            // Skip empty lines and comments
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            // Parse KEY=VALUE format
            if let Some(eq_pos) = line.find('=') {
                let key = line[..eq_pos].trim();
                let value = line[eq_pos + 1..].trim();

                if key.is_empty() {
                    return Err(CliError::Config(format!(
                        "Invalid env file: empty key at line {}",
                        line_num + 1
                    )));
                }

                // Remove quotes from value if present
                let value = Self::unquote_value(value);

                self.vars.insert(key.to_string(), value);
            } else {
                return Err(CliError::Config(format!(
                    "Invalid env file: malformed line {} (expected KEY=VALUE)",
                    line_num + 1
                )));
            }
        }

        Ok(())
    }

    fn unquote_value(value: &str) -> String {
        let value = value.trim();

        // Handle double quotes
        if value.starts_with('"') && value.ends_with('"') && value.len() >= 2 {
            return value[1..value.len() - 1].to_string();
        }

        // Handle single quotes
        if value.starts_with('\'') && value.ends_with('\'') && value.len() >= 2 {
            return value[1..value.len() - 1].to_string();
        }

        value.to_string()
    }

    /// Default patterns for sensitive variable detection
    fn default_sensitive_patterns() -> Vec<String> {
        vec![
            "password".to_string(),
            "passwd".to_string(),
            "secret".to_string(),
            "token".to_string(),
            "key".to_string(),
            "api_key".to_string(),
            "apikey".to_string(),
            "auth".to_string(),
            "credential".to_string(),
            "private".to_string(),
        ]
    }
}

impl Default for EnvManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_basic_env() {
        let mut env = EnvManager {
            vars: HashMap::new(),
            _sensitive_patterns: Vec::new(),
        };
        let content = r#"
# Comment
KEY1=value1
KEY2=value2
        "#;

        env.parse_env_content(content).unwrap();
        assert_eq!(env.vars.get("KEY1").unwrap(), "value1");
        assert_eq!(env.vars.get("KEY2").unwrap(), "value2");
    }

    #[test]
    fn test_parse_quoted_values() {
        let mut env = EnvManager {
            vars: HashMap::new(),
            _sensitive_patterns: Vec::new(),
        };
        let content = r#"
QUOTED="value with spaces"
SINGLE='single quoted'
UNQUOTED=no_spaces
        "#;

        env.parse_env_content(content).unwrap();
        assert_eq!(env.vars.get("QUOTED").unwrap(), "value with spaces");
        assert_eq!(env.vars.get("SINGLE").unwrap(), "single quoted");
        assert_eq!(env.vars.get("UNQUOTED").unwrap(), "no_spaces");
    }

    #[test]
    fn test_invalid_env_format() {
        let mut env = EnvManager {
            vars: HashMap::new(),
            _sensitive_patterns: Vec::new(),
        };
        let content = "INVALID LINE WITHOUT EQUALS";
        assert!(env.parse_env_content(content).is_err());
    }
}
