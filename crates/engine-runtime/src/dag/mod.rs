use crate::dag::builder::PipelineNode;
use std::collections::HashMap;

pub mod builder;
pub mod error;
pub mod executor;

#[derive(Debug, Clone)]
pub struct Dag {
    nodes: HashMap<String, PipelineNode>,
    /// Execution order grouped by levels (each level can run in parallel)
    execution_order: Vec<Vec<String>>,
}

impl Dag {
    pub fn execution_order(&self) -> &Vec<Vec<String>> {
        &self.execution_order
    }

    pub fn total_pipelines(&self) -> usize {
        self.nodes.len()
    }

    pub fn max_parallelism(&self) -> usize {
        self.execution_order
            .iter()
            .map(|level| level.len())
            .max()
            .unwrap_or(0)
    }
}
