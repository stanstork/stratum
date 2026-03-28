use crate::schema::dep_graph::DependencyGraph;
use connectors::sql::metadata::table::TableMetadata;
use model::{
    execution::{pipeline::Pipeline, references::DataMode},
    transform::mapping::TransformationMetadata,
};
use std::collections::HashMap;

/// Topologically sort cascade tables so FK-referenced tables come before the
/// tables that reference them. Falls back to arbitrary order on cycles.
pub fn topological_sort_tables(tables: &HashMap<String, TableMetadata>) -> Vec<String> {
    let mut graph = DependencyGraph::new();
    for (name, meta) in tables {
        graph.add_table(name.clone());
        for fk in &meta.foreign_keys {
            if tables.contains_key(&fk.referenced_table) {
                graph.add_dependency(name.clone(), fk.referenced_table.clone());
            }
        }
    }
    graph
        .without_self_references()
        .topological_order()
        .unwrap_or_else(|_| tables.keys().cloned().collect())
}

/// Map source cascade table names to their destination equivalents and return
/// them in topological (FK-safe) order. Returns an empty vec for non-cascade pipelines.
pub fn resolve_cascade_tables(
    pipeline: &Pipeline,
    mapping: &TransformationMetadata,
    cascade_meta: &Option<HashMap<String, TableMetadata>>,
) -> Vec<String> {
    if let Some(refs) = &pipeline.source.graph_references
        && matches!(refs.data_mode, DataMode::Cascade)
        && let Some(meta) = cascade_meta
    {
        return topological_sort_tables(meta)
            .into_iter()
            .map(|src_name| mapping.entities.resolve(&src_name))
            .collect();
    }
    vec![]
}
