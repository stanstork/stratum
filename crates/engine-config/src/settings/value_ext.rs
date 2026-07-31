use model::core::value::Value;
use std::collections::HashMap;

/// Extension trait for typed value extraction from HashMap<String, Value>.
///
/// Provides a cleaner API for parsing settings maps into typed values,
/// avoiding repetitive pattern matching.
pub trait CanonicalValueMapExt {
    /// Extract a boolean value, returning None if the key doesn't exist or has wrong type.
    fn get_bool(&self, key: &str) -> Option<bool>;

    /// Extract a usize value from various numeric types, returning None if negative or wrong type.
    fn get_usize(&self, key: &str) -> Option<usize>;
}

impl CanonicalValueMapExt for HashMap<String, Value> {
    fn get_bool(&self, key: &str) -> Option<bool> {
        self.get(key).and_then(|v| match v {
            Value::Boolean(b) => Some(*b),
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
}
