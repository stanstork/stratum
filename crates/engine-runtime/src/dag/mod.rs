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

    pub fn get_dependencies(&self, pipeline_name: &str) -> Option<Vec<String>> {
        if !self.nodes.contains_key(pipeline_name) {
            return None;
        }

        let mut all_dependencies = Vec::new();
        let mut visited = std::collections::HashSet::new();

        self.collect_dependencies(pipeline_name, &mut all_dependencies, &mut visited);

        Some(all_dependencies)
    }

    /// Recursively collects all dependencies in depth-first order
    fn collect_dependencies(
        &self,
        pipeline_name: &str,
        result: &mut Vec<String>,
        visited: &mut std::collections::HashSet<String>,
    ) {
        if let Some(node) = self.nodes.get(pipeline_name) {
            for dep in &node.dependencies {
                if !visited.contains(dep) {
                    self.collect_dependencies(dep, result, visited);

                    visited.insert(dep.clone());
                    result.push(dep.clone());
                }
            }
        }
    }
}
