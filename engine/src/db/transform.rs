use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Transformation {
    Simple(String),
    Function { function: String, args: Vec<String> },
}
