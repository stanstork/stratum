use crate::{core::value::Value, execution::properties::Properties};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Connection block compiled to runtime configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Connection {
    pub name: String,
    pub driver: String,
    pub properties: Properties,
    pub nested_configs: HashMap<String, HashMap<String, Value>>,
}
