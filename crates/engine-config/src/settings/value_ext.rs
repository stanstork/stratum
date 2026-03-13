use model::core::value::Value;
use std::collections::HashMap;

/// Extension trait for typed value extraction from HashMap<String, Value>.
///
/// Provides a cleaner API for parsing settings maps into typed values,
/// avoiding repetitive pattern matching.
pub trait CanonicalValueMapExt {
    /// Extract a boolean value, returning None if the key doesn't exist or has wrong type.
    fn get_bool(&self, key: &str) -> Option<bool>;

    /// Extract a string value, returning None if the key doesn't exist or has wrong type.
    fn get_string(&self, key: &str) -> Option<String>;

    /// Extract a usize value from various numeric types, returning None if negative or wrong type.
    fn get_usize(&self, key: &str) -> Option<usize>;

    /// Extract the first character from a string value.
    fn get_char(&self, key: &str) -> Option<char>;
}

impl CanonicalValueMapExt for HashMap<String, Value> {
    fn get_bool(&self, key: &str) -> Option<bool> {
        self.get(key).and_then(|v| match v {
            Value::Boolean(b) => Some(*b),
            _ => None,
        })
    }

    fn get_string(&self, key: &str) -> Option<String> {
        self.get(key).and_then(|v| match v {
            Value::String(s) => Some(s.clone()),
            _ => None,
        })
    }

    fn get_usize(&self, key: &str) -> Option<usize> {
        self.get(key).and_then(|v| match v {
            Value::Int(i) if *i >= 0 => Some(*i as usize),
            Value::UInt(u) => Some(*u as usize),
            Value::Float(f) if *f >= 0.0 => Some(*f as usize),
            _ => None,
        })
    }

    fn get_char(&self, key: &str) -> Option<char> {
        self.get(key).and_then(|v| match v {
            Value::String(s) => s.chars().next(),
            _ => None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_bool() {
        let mut map = HashMap::new();
        map.insert("enabled".to_string(), Value::Boolean(true));
        map.insert("disabled".to_string(), Value::Boolean(false));

        assert_eq!(map.get_bool("enabled"), Some(true));
        assert_eq!(map.get_bool("disabled"), Some(false));
        assert_eq!(map.get_bool("missing"), None);
    }

    #[test]
    fn test_get_bool_wrong_type() {
        let mut map = HashMap::new();
        map.insert("not_bool".to_string(), Value::Int(42));

        assert_eq!(map.get_bool("not_bool"), None);
    }

    #[test]
    fn test_get_string() {
        let mut map = HashMap::new();
        map.insert("text".to_string(), Value::String("hello".to_string()));
        map.insert("varchar".to_string(), Value::String("world".to_string()));

        assert_eq!(map.get_string("text"), Some("hello".to_string()));
        assert_eq!(map.get_string("varchar"), Some("world".to_string()));
        assert_eq!(map.get_string("missing"), None);
    }

    #[test]
    fn test_get_usize_from_various_types() {
        let mut map = HashMap::new();
        map.insert("int".to_string(), Value::Int(42));
        map.insert("uint".to_string(), Value::UInt(200));
        map.insert("float".to_string(), Value::Float(300.5));

        assert_eq!(map.get_usize("int"), Some(42));
        assert_eq!(map.get_usize("uint"), Some(200));
        assert_eq!(map.get_usize("float"), Some(300));
    }

    #[test]
    fn test_get_usize_rejects_negative() {
        let mut map = HashMap::new();
        map.insert("negative".to_string(), Value::Int(-42));

        assert_eq!(map.get_usize("negative"), None);
    }

    #[test]
    fn test_get_char() {
        let mut map = HashMap::new();
        map.insert("comma".to_string(), Value::String(",".to_string()));
        map.insert("letter".to_string(), Value::String("abc".to_string()));

        assert_eq!(map.get_char("comma"), Some(','));
        assert_eq!(map.get_char("letter"), Some('a'));
        assert_eq!(map.get_char("missing"), None);
    }

    #[test]
    fn test_get_char_empty_string() {
        let mut map = HashMap::new();
        map.insert("empty".to_string(), Value::String("".to_string()));

        assert_eq!(map.get_char("empty"), None);
    }
}
