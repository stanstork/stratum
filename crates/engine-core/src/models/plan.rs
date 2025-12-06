use crate::models::{connection::Connection, define::GlobalDefinitions, pipeline::Pipeline};
use serde::{Deserialize, Serialize};

/// Top-level execution plan compiled from SMQL AST
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionPlan {
    pub definitions: GlobalDefinitions,
    pub connections: Vec<Connection>,
    pub pipelines: Vec<Pipeline>,
}
