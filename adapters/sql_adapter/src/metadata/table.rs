use super::{column::metadata::ColumnMetadata, foreign_key::ForeignKeyMetadata};
use crate::{adapter::DbAdapter, mysql::MySqlAdapter};
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
};

#[derive(Debug, Clone)]
pub struct TableMetadata {
    pub name: String,
    pub schema: Option<String>,
    pub columns: HashMap<String, ColumnMetadata>,
    pub primary_keys: Vec<String>,
    pub foreign_keys: Vec<ForeignKeyMetadata>,
    pub referenced_tables: HashMap<String, TableMetadata>,
    pub referencing_tables: HashMap<String, TableMetadata>,
}

impl TableMetadata {
    pub fn build_dep_graph<'a>(
        table_name: &'a str,
        manager: &'a MySqlAdapter,
        graph: &'a mut HashMap<String, TableMetadata>,
        visited: &'a mut HashSet<String>,
    ) -> Pin<Box<dyn Future<Output = Result<TableMetadata, Box<dyn std::error::Error>>> + 'a>> {
        Box::pin(async move {
            if let Some(metadata) = graph.get(table_name) {
                return Ok(metadata.clone());
            }

            if !visited.insert(table_name.to_string()) {
                return Err("Circular reference detected".into());
            }

            let mut metadata = manager.fetch_metadata(table_name).await?;
            graph.insert(table_name.to_string(), metadata.clone());

            for fk in &metadata.foreign_keys {
                let ref_table = &fk.referenced_table;

                if !graph.contains_key(ref_table) {
                    let ref_metadata =
                        Self::build_dep_graph(ref_table, manager, graph, visited).await?;

                    metadata
                        .referenced_tables
                        .insert(ref_table.clone(), ref_metadata.clone());

                    // **Bidirectional Relationship: Link referencing tables**
                    graph
                        .entry(ref_table.clone())
                        .and_modify(|t| {
                            t.referencing_tables
                                .insert(table_name.to_string(), metadata.clone());
                        })
                        .or_insert_with(|| {
                            let mut t = ref_metadata.clone();
                            t.referencing_tables
                                .insert(table_name.to_string(), metadata.clone());
                            t
                        });
                }
            }

            graph.insert(table_name.to_string(), metadata.clone());

            Ok(metadata)
        })
    }
}
