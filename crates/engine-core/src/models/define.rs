use model::core::value::Value;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Global definitions from define {} block
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalDefinitions {
    pub variables: HashMap<String, Value>,
}
