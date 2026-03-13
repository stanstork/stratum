use crate::error::DependencyError;
use std::collections::{HashMap, HashSet, VecDeque};
use tracing::warn;

/// Represents a directed graph of table dependencies based on foreign keys
#[derive(Debug, Clone, Default)]
pub struct DependencyGraph {
    /// Adjacency list: table -> tables it depends on (referenced tables)
    dependencies: HashMap<String, HashSet<String>>,

    /// Reverse adjacency list: table -> tables that depend on it
    dependents: HashMap<String, HashSet<String>>,

    /// All tables in the graph
    tables: HashSet<String>,
}

impl DependencyGraph {
    pub fn new() -> Self {
        Self {
            dependencies: HashMap::new(),
            dependents: HashMap::new(),
            tables: HashSet::new(),
        }
    }

    /// Add a table without any dependencies
    pub fn add_table(&mut self, table: String) {
        self.tables.insert(table.clone());
        self.dependencies.entry(table.clone()).or_default();
        self.dependents.entry(table).or_default();
    }

    /// Add a dependency: from_table depends on to_table (FK relationship)
    pub fn add_dependency(&mut self, from_table: String, to_table: String) {
        // Add both tables
        self.tables.insert(from_table.clone());
        self.tables.insert(to_table.clone());

        // Forward edge: from -> to
        self.dependencies
            .entry(from_table.clone())
            .or_default()
            .insert(to_table.clone());

        // Ensure to_table has an entry in dependencies (even if empty)
        self.dependencies.entry(to_table.clone()).or_default();

        // Reverse edge: to <- from
        self.dependents
            .entry(to_table)
            .or_default()
            .insert(from_table);
    }

    /// Get tables that a given table depends on
    pub fn get_dependencies(&self, table: &str) -> Option<&HashSet<String>> {
        self.dependencies.get(table)
    }

    /// Get tables that depend on a given table
    pub fn get_dependents(&self, table: &str) -> Option<&HashSet<String>> {
        self.dependents.get(table)
    }

    /// Compute topological order using Kahn's algorithm
    /// Returns tables in dependency order (dependencies come before dependents)
    pub fn topological_order(&self) -> Result<Vec<String>, DependencyError> {
        let mut in_degree = HashMap::new();
        for table in &self.tables {
            let degree = self
                .dependencies
                .get(table)
                .map(|deps| deps.len())
                .unwrap_or(0);
            in_degree.insert(table.clone(), degree);
        }

        let mut queue = VecDeque::new();
        for (table, &degree) in &in_degree {
            if degree == 0 {
                queue.push_back(table.clone());
            }
        }

        let mut result = Vec::new();

        while let Some(table) = queue.pop_front() {
            result.push(table.clone());

            if let Some(dependents) = self.dependents.get(&table) {
                for dependent in dependents {
                    if let Some(degree) = in_degree.get_mut(dependent) {
                        *degree -= 1;
                        if *degree == 0 {
                            queue.push_back(dependent.clone());
                        }
                    }
                }
            }
        }

        // If we didn't process all tables, there's a cycle
        if result.len() != self.tables.len() {
            let unprocessed: Vec<&String> =
                self.tables.iter().filter(|t| !result.contains(t)).collect();

            return Err(DependencyError::CircularDependency(format!(
                "Tables involved in cycle: {:?}",
                unprocessed
            )));
        }

        Ok(result)
    }

    /// Compute reverse topological order (for DROP operations)
    pub fn reverse_topological_order(&self) -> Result<Vec<String>, DependencyError> {
        let mut forward = self.topological_order()?;
        forward.reverse();
        Ok(forward)
    }

    /// Validate that all dependencies exist
    pub fn validate(&self) -> Result<(), DependencyError> {
        for (table, deps) in &self.dependencies {
            for dep in deps {
                if !self.tables.contains(dep) {
                    return Err(DependencyError::MissingDependency {
                        table: table.clone(),
                        dependency: dep.clone(),
                    });
                }
            }
        }
        Ok(())
    }

    /// Group tables into levels where each level can be processed in parallel
    /// Level 0: no dependencies
    /// Level 1: depends only on level 0
    /// Level 2: depends on level 0 or 1, etc.
    pub fn execution_levels(&self) -> Result<Vec<Vec<String>>, DependencyError> {
        let topo_order = self.topological_order()?;
        let mut levels: Vec<Vec<String>> = Vec::new();
        let mut table_levels: HashMap<String, usize> = HashMap::new();

        for table in topo_order {
            // Find max level of dependencies.
            // All tables in self.tables always have an entry in self.dependencies
            // (invariant maintained by add_table/add_dependency), but we use
            // unwrap_or_default to be safe against future API changes.
            let empty = HashSet::new();
            let deps = self.dependencies.get(&table).unwrap_or_else(|| {
                warn!(
                    "Table '{}' missing from dependency map; treating as no dependencies",
                    table
                );
                &empty
            });
            let max_dep_level = deps
                .iter()
                .filter_map(|dep| table_levels.get(dep))
                .max()
                .unwrap_or(&0);

            let level = if deps.is_empty() {
                0
            } else {
                max_dep_level + 1
            };

            table_levels.insert(table.clone(), level);

            // Ensure levels vec is large enough
            while levels.len() <= level {
                levels.push(Vec::new());
            }

            levels[level].push(table);
        }

        Ok(levels)
    }

    /// Like `topological_order()` but handles cycles gracefully.
    ///
    /// Acyclically-ordered tables come first; any remaining tables that are part
    /// of a cycle are appended in alphabetical order. The result is always
    /// deterministic, unlike falling back to HashMap iteration order.
    pub fn partial_topological_order(&self) -> Vec<String> {
        let mut in_degree: HashMap<String, usize> = self
            .tables
            .iter()
            .map(|t| {
                let deg = self.dependencies.get(t).map(|d| d.len()).unwrap_or(0);
                (t.clone(), deg)
            })
            .collect();

        // Seed the queue with zero-degree tables, sorted for determinism.
        let mut queue: VecDeque<String> = {
            let mut seeds: Vec<String> = in_degree
                .iter()
                .filter(|&(_, d)| *d == 0)
                .map(|(t, _)| t.clone())
                .collect();
            seeds.sort();
            seeds.into()
        };

        let mut result = Vec::with_capacity(self.tables.len());

        while let Some(table) = queue.pop_front() {
            result.push(table.clone());

            if let Some(dependents) = self.dependents.get(&table) {
                let mut next: Vec<String> = dependents
                    .iter()
                    .filter_map(|dep| {
                        let deg = in_degree.get_mut(dep)?;
                        *deg -= 1;
                        if *deg == 0 { Some(dep.clone()) } else { None }
                    })
                    .collect();
                next.sort();
                queue.extend(next);
            }
        }

        // Append cycle members alphabetically for determinism.
        if result.len() < self.tables.len() {
            let in_result: HashSet<&str> = result.iter().map(String::as_str).collect();
            let mut cyclic: Vec<String> = self
                .tables
                .iter()
                .filter(|t| !in_result.contains(t.as_str()))
                .cloned()
                .collect();
            cyclic.sort();
            result.extend(cyclic);
        }

        result
    }

    /// Get self-referencing tables (tables with FKs to themselves)
    pub fn self_referencing_tables(&self) -> HashSet<String> {
        let mut result = HashSet::new();

        for (table, deps) in &self.dependencies {
            if deps.contains(table) {
                result.insert(table.clone());
            }
        }

        result
    }

    /// Build graph without self-references (for initial schema creation)
    pub fn without_self_references(&self) -> Self {
        let mut new_graph = Self::new();

        for table in &self.tables {
            new_graph.add_table(table.clone());
        }

        for (from, tos) in &self.dependencies {
            for to in tos {
                // Skip self-references
                if from != to {
                    new_graph.add_dependency(from.clone(), to.clone());
                }
            }
        }

        new_graph
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_chain() {
        let mut graph = DependencyGraph::new();

        // A -> B -> C (A depends on B, B depends on C)
        graph.add_dependency("A".to_string(), "B".to_string());
        graph.add_dependency("B".to_string(), "C".to_string());

        let order = graph.topological_order().unwrap();

        // C should come before B, B before A
        let c_idx = order.iter().position(|t| t == "C").unwrap();
        let b_idx = order.iter().position(|t| t == "B").unwrap();
        let a_idx = order.iter().position(|t| t == "A").unwrap();

        assert!(c_idx < b_idx);
        assert!(b_idx < a_idx);
    }

    #[test]
    fn test_diamond_dependency() {
        let mut graph = DependencyGraph::new();

        //     D
        //    / \
        //   B   C
        //    \ /
        //     A
        graph.add_dependency("A".to_string(), "B".to_string());
        graph.add_dependency("A".to_string(), "C".to_string());
        graph.add_dependency("B".to_string(), "D".to_string());
        graph.add_dependency("C".to_string(), "D".to_string());

        let order = graph.topological_order().unwrap();

        let d_idx = order.iter().position(|t| t == "D").unwrap();
        let b_idx = order.iter().position(|t| t == "B").unwrap();
        let c_idx = order.iter().position(|t| t == "C").unwrap();
        let a_idx = order.iter().position(|t| t == "A").unwrap();

        // D comes before both B and C
        assert!(d_idx < b_idx);
        assert!(d_idx < c_idx);

        // Both B and C come before A
        assert!(b_idx < a_idx);
        assert!(c_idx < a_idx);
    }

    #[test]
    fn test_circular_dependency() {
        let mut graph = DependencyGraph::new();

        // A -> B -> C -> A (circular)
        graph.add_dependency("A".to_string(), "B".to_string());
        graph.add_dependency("B".to_string(), "C".to_string());
        graph.add_dependency("C".to_string(), "A".to_string());

        let result = graph.topological_order();

        assert!(result.is_err());
        if let Err(DependencyError::CircularDependency(msg)) = result {
            assert!(msg.contains("A") && msg.contains("B") && msg.contains("C"));
        }
    }

    #[test]
    fn test_execution_levels() {
        let mut graph = DependencyGraph::new();

        // Level 0: D
        // Level 1: B, C (both depend only on D)
        // Level 2: A (depends on B and C)
        graph.add_dependency("A".to_string(), "B".to_string());
        graph.add_dependency("A".to_string(), "C".to_string());
        graph.add_dependency("B".to_string(), "D".to_string());
        graph.add_dependency("C".to_string(), "D".to_string());

        let levels = graph.execution_levels().unwrap();

        assert_eq!(levels.len(), 3);
        assert_eq!(levels[0], vec!["D"]);
        assert!(levels[1].contains(&"B".to_string()));
        assert!(levels[1].contains(&"C".to_string()));
        assert_eq!(levels[2], vec!["A"]);
    }

    #[test]
    fn test_partial_topological_order_with_cycle() {
        let mut graph = DependencyGraph::new();

        // A <-> B is a cycle; C depends on A (so C comes after A in partial order)
        graph.add_dependency("A".to_string(), "B".to_string());
        graph.add_dependency("B".to_string(), "A".to_string());
        graph.add_dependency("C".to_string(), "A".to_string());

        let order = graph.partial_topological_order();

        // All tables present
        assert_eq!(order.len(), 3);
        assert!(order.contains(&"A".to_string()));
        assert!(order.contains(&"B".to_string()));
        assert!(order.contains(&"C".to_string()));

        // C must come after A (A is a dependency of C)
        let a_idx = order.iter().position(|t| t == "A").unwrap();
        let c_idx = order.iter().position(|t| t == "C").unwrap();
        assert!(a_idx < c_idx, "A must precede C");
    }

    #[test]
    fn test_partial_topological_order_no_cycle() {
        let mut graph = DependencyGraph::new();
        graph.add_dependency("A".to_string(), "B".to_string());
        graph.add_dependency("B".to_string(), "C".to_string());

        let order = graph.partial_topological_order();
        let c_idx = order.iter().position(|t| t == "C").unwrap();
        let b_idx = order.iter().position(|t| t == "B").unwrap();
        let a_idx = order.iter().position(|t| t == "A").unwrap();
        assert!(c_idx < b_idx && b_idx < a_idx);
    }

    #[test]
    fn test_self_reference() {
        let mut graph = DependencyGraph::new();

        // A references itself (e.g., parent_id FK to same table)
        graph.add_dependency("A".to_string(), "A".to_string());

        let result = graph.topological_order();

        // Self-reference creates a cycle
        assert!(result.is_err());
    }
}
