use crate::plan::connection::utils::mask_url;
use model::core::value::Value;

/// Policy for masking sensitive data in pipeline plans and samples
#[derive(Debug, Clone)]
pub struct MaskingPolicy {
    /// Automatically mask columns/variables with sensitive names
    pub auto_mask_sensitive: bool,
    /// Explicitly mask these column/variable names
    pub explicit_mask: Vec<String>,
}

impl MaskingPolicy {
    pub fn new(auto_mask_sensitive: bool, explicit_mask: Vec<String>) -> Self {
        Self {
            auto_mask_sensitive,
            explicit_mask,
        }
    }

    pub fn permissive() -> Self {
        Self {
            auto_mask_sensitive: false,
            explicit_mask: Vec::new(),
        }
    }

    pub fn strict(explicit_mask: Vec<String>) -> Self {
        Self {
            auto_mask_sensitive: true,
            explicit_mask,
        }
    }

    pub fn should_mask(&self, name: &str) -> bool {
        if self
            .explicit_mask
            .iter()
            .any(|m| m.eq_ignore_ascii_case(name))
        {
            return true;
        }

        if self.auto_mask_sensitive {
            let lower_name = name.to_lowercase();

            // Common patterns for sensitive data
            let sensitive_patterns = [
                "password",
                "passwd",
                "pwd",
                "secret",
                "token",
                "key",
                "api_key",
                "apikey",
                "auth",
                "credential",
                "private",
                "salt",
                "hash",
                "certificate",
                "cert",
                "ssn",
                "social_security",
                "credit_card",
                "card_number",
            ];

            for pattern in &sensitive_patterns {
                if lower_name.contains(pattern) {
                    return true;
                }
            }
        }

        false
    }

    /// Check if a value looks like a database connection URL with credentials
    pub fn is_db_url(value: &str) -> bool {
        let db_prefixes = [
            "mysql://",
            "postgres://",
            "postgresql://",
            "mariadb://",
            "sqlite://",
            "mssql://",
            "sqlserver://",
            "oracle://",
        ];

        let lower = value.to_lowercase();
        db_prefixes.iter().any(|prefix| lower.starts_with(prefix)) && value.contains('@')
    }

    /// Mask a database URL if it contains credentials
    pub fn mask_url(value: &str) -> String {
        if Self::is_db_url(value) {
            mask_url(value)
        } else {
            value.to_string()
        }
    }

    /// Mask a string value while preserving some context
    pub fn mask_value(&self, value: &str) -> String {
        if value.is_empty() {
            return "***".to_string();
        }

        let len = value.len();

        // For very short values (1-3 chars), mask completely
        if len <= 3 {
            return "*".repeat(len);
        }

        // For 4-8 chars: show first char, mask middle, show last char
        if len <= 8 {
            let first = value.chars().next().unwrap();
            let last = value.chars().last().unwrap();
            format!("{}{}{}{}{}", first, "*", "*", "*", last)
        } else {
            // For 9+ chars: show first 2 chars, mask middle with fixed asterisks, show last 2 chars
            let chars: Vec<char> = value.chars().collect();
            let first_two: String = chars[..2].iter().collect();
            let last_two: String = chars[len - 2..].iter().collect();
            format!("{}******{}", first_two, last_two)
        }
    }

    /// Mask a Value type if needed based on the name or if it's a database URL
    pub fn mask_by_name(&self, name: &str, value: &Value) -> String {
        let value_str = value.as_string().unwrap_or_else(|| value.to_string());

        // Always mask database URLs with credentials
        if Self::is_db_url(&value_str) {
            return Self::mask_url(&value_str);
        }

        if self.should_mask(name) {
            self.mask_value(&value_str)
        } else {
            value_str
        }
    }

    pub fn mask_env_var_value(&self, var_name: &str, value: &Value) -> Option<String> {
        Some(self.mask_by_name(var_name, value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_mask_explicit() {
        let policy = MaskingPolicy::new(false, vec!["user_id".to_string()]);
        assert!(policy.should_mask("user_id"));
        assert!(policy.should_mask("USER_ID")); // Case insensitive
        assert!(!policy.should_mask("username"));
    }

    #[test]
    fn test_should_mask_auto_sensitive() {
        let policy = MaskingPolicy::strict(vec![]);

        assert!(policy.should_mask("password"));
        assert!(policy.should_mask("api_key"));
        assert!(policy.should_mask("user_password"));
        assert!(policy.should_mask("API_SECRET_KEY"));
        assert!(!policy.should_mask("username"));
        assert!(!policy.should_mask("user_id"));
    }

    #[test]
    fn test_should_mask_permissive() {
        let policy = MaskingPolicy::permissive();

        assert!(!policy.should_mask("password"));
        assert!(!policy.should_mask("api_key"));
        assert!(!policy.should_mask("anything"));
    }

    #[test]
    fn test_mask_value_short() {
        let policy = MaskingPolicy::permissive();

        assert_eq!(policy.mask_value("abc"), "***");
        assert_eq!(policy.mask_value("a"), "*");
        assert_eq!(policy.mask_value(""), "***");
    }

    #[test]
    fn test_mask_value_medium() {
        let policy = MaskingPolicy::permissive();

        assert_eq!(policy.mask_value("test"), "t***t");
        assert_eq!(policy.mask_value("password"), "p***d");
    }

    #[test]
    fn test_mask_value_long() {
        let policy = MaskingPolicy::permissive();

        assert_eq!(policy.mask_value("verylongpassword123"), "ve******23");
        assert_eq!(policy.mask_value("my_secret_key_value"), "my******ue");
    }

    #[test]
    fn test_mask_value_if_needed() {
        let policy = MaskingPolicy::strict(vec!["custom".to_string()]);

        let value = Value::String("sensitive_data".to_string());

        // Should mask because of 'password' in name
        let masked = policy.mask_by_name("user_password", &value);
        assert_eq!(masked, "se******ta");

        // Should mask because of explicit list
        let masked = policy.mask_by_name("custom", &value);
        assert_eq!(masked, "se******ta");

        // Should not mask
        let unmasked = policy.mask_by_name("username", &value);
        assert_eq!(unmasked, "sensitive_data");
    }
}
