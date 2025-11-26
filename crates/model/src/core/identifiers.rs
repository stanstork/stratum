use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RunId(Arc<str>);

impl RunId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(Arc::from(id.into()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for RunId {
    fn from(s: String) -> Self {
        Self::new(s)
    }
}

impl From<&str> for RunId {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ItemId(Arc<str>);

impl ItemId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(Arc::from(id.into()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for ItemId {
    fn from(s: String) -> Self {
        Self::new(s)
    }
}

impl From<&str> for ItemId {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PartId(Arc<str>);

impl PartId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(Arc::from(id.into()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for PartId {
    fn from(s: String) -> Self {
        Self::new(s)
    }
}

impl From<&str> for PartId {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BatchId(Arc<str>);

impl BatchId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(Arc::from(id.into()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for BatchId {
    fn from(s: String) -> Self {
        Self::new(s)
    }
}

impl From<&str> for BatchId {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}
