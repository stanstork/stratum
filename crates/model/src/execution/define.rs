use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::core::value::Value;

/// Global definitions from define {} block
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalDefinitions {
    pub variables: HashMap<String, Value>,
}
