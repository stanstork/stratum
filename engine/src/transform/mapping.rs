use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TransformMapping {
    Function { function: String, args: Vec<String> },
}
