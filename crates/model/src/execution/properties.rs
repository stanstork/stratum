use crate::core::value::Value;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Properties {
    inner: HashMap<String, Value>,
}

impl Properties {
    pub fn new() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }

    pub fn insert(&mut self, key: String, value: Value) {
        self.inner.insert(key, value);
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        self.inner.get(key)
    }

    pub fn get_string(&self, key: &str) -> Option<String> {
        self.inner.get(key).and_then(|v| match v {
            Value::String(s) => Some(s.clone()),
            _ => None,
        })
    }

    pub fn get_bool(&self, key: &str) -> Option<bool> {
        self.inner.get(key).and_then(|v| match v {
            Value::Boolean(b) => Some(*b),
            _ => None,
        })
    }

    pub fn get_usize(&self, key: &str) -> Option<usize> {
        self.inner.get(key).and_then(|v| match v {
            Value::Usize(n) => Some(*n),
            _ => None,
        })
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }
}
