use crate::dag::{Dag, error::DagError};
use std::collections::{HashMap, HashSet, VecDeque};

#[derive(Debug, Clone)]
pub struct PipelineNode {
    pub name: String,
    pub dependencies: Vec<String>,
}

pub struct DagBuilder {
    pub nodes: HashMap<String, PipelineNode>,
}

impl DagBuilder {
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
        }
    }

    pub fn add_pipeline(
        &mut self,
        name: String,
        dependencies: Vec<String>,
    ) -> Result<(), DagError> {
        if self.nodes.contains_key(&name) {
            return Err(DagError::DuplicatePipeline(name));
        }

        let node = PipelineNode {
            name: name.clone(),
            dependencies,
        };
        self.nodes.insert(name, node);

        Ok(())
    }

    pub fn build(&self) -> Result<Dag, DagError> {
        if self.nodes.is_empty() {
            return Err(DagError::EmptyPipelines);
        }

        // Validate all dependencies exist
        for (name, node) in &self.nodes {
            for dep in &node.dependencies {
                if !self.nodes.contains_key(dep) {
                    return Err(DagError::MissingDependency {
                        pipeline: name.clone(),
                        dependency: dep.clone(),
                    });
                }
            }
        }

        self.detect_cycles()?;

        // Perform topological sort to determine execution order
        let execution_order = self.topological_sort()?;

        Ok(Dag {
            nodes: self.nodes.clone(),
            execution_order,
        })
    }

    fn detect_cycles(&self) -> Result<(), DagError> {
        let mut visited = HashSet::new();
        let mut stack = HashSet::new();

        for node in self.nodes.keys() {
            if !visited.contains(node) {
                self.dfs_cycle_detect(node, &mut visited, &mut stack)?;
            }
        }

        Ok(())
    }

    fn dfs_cycle_detect(
        &self,
        node: &String,
        visited: &mut HashSet<String>,
        rec_stack: &mut HashSet<String>,
    ) -> Result<(), DagError> {
        visited.insert(node.to_string());
        rec_stack.insert(node.to_string());

        if let Some(pipeline_node) = self.nodes.get(node) {
            for dep in &pipeline_node.dependencies {
                if !visited.contains(dep) {
                    self.dfs_cycle_detect(dep, visited, rec_stack)?;
                } else if rec_stack.contains(dep) {
                    return Err(DagError::CircularDependency(format!(
                        "{} -> {} (cycle detected)",
                        node, dep
                    )));
                }
            }
        }

        rec_stack.remove(node);
        Ok(())
    }

    fn topological_sort(&self) -> Result<Vec<Vec<String>>, DagError> {
        let mut in_degree: HashMap<String, usize> = HashMap::new();
        let mut adjacency: HashMap<String, Vec<String>> = HashMap::new();

        // Initialize in-degrees and adjacency list
        for (name, node) in &self.nodes {
            in_degree.entry(name.clone()).or_insert(0);
            adjacency.entry(name.clone()).or_default();

            for dep in &node.dependencies {
                *in_degree.entry(name.clone()).or_insert(0) += 1;
                adjacency.entry(dep.clone()).or_default().push(name.clone());
            }
        }

        // Find all nodes with in-degree of 0 (no dependencies)
        let mut queue: VecDeque<String> = in_degree
            .iter()
            .filter(|&(_, &degree)| degree == 0)
            .map(|(name, _)| name.clone())
            .collect();

        let mut execution_order: Vec<Vec<String>> = Vec::new();
        let mut processed = 0;

        while !queue.is_empty() {
            // Nodes that can be executed in parallel
            let current_level: Vec<String> = queue.drain(..).collect();
            execution_order.push(current_level.clone());
            processed += current_level.len();

            // Process neighbors
            for node in current_level {
                if let Some(neighbors) = adjacency.get(&node) {
                    for neighbor in neighbors {
                        if let Some(degree) = in_degree.get_mut(neighbor) {
                            *degree -= 1;
                            if *degree == 0 {
                                queue.push_back(neighbor.clone());
                            }
                        }
                    }
                }
            }
        }

        // If not all nodes processed, there's a cycle
        if processed != self.nodes.len() {
            return Err(DagError::CircularDependency(
                "Unexpected cycle during topological sort".to_string(),
            ));
        }

        Ok(execution_order)
    }
}

impl Default for DagBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sequential_dependencies() {
        // copy_actors (no deps) → copy_customers → copy_film
        let mut builder = DagBuilder::new();
        builder
            .add_pipeline("copy_actors".to_string(), vec![])
            .unwrap();
        builder
            .add_pipeline(
                "copy_customers".to_string(),
                vec!["copy_actors".to_string()],
            )
            .unwrap();
        builder
            .add_pipeline("copy_film".to_string(), vec!["copy_customers".to_string()])
            .unwrap();

        let dag = builder.build().unwrap();

        println!("Execution order: {:?}", dag.execution_order);

        // Should have 3 levels for sequential dependencies
        assert_eq!(
            dag.execution_order.len(),
            3,
            "Expected 3 levels, got {}",
            dag.execution_order.len()
        );
        assert_eq!(dag.execution_order[0], vec!["copy_actors"]);
        assert_eq!(dag.execution_order[1], vec!["copy_customers"]);
        assert_eq!(dag.execution_order[2], vec!["copy_film"]);
    }
}
