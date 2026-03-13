use super::{column::ColumnMetadata, fk::ForeignKeyMetadata, table::TableMetadata};
use crate::{error::DriverError, traits::introspector::SchemaIntrospector};
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
};

// MetadataFuture is a type alias for a Future that returns a Result
// containing the TableMetadata or an error
pub type MetadataFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, DriverError>> + Send + 'a>>;

pub struct MetadataProvider;

impl MetadataProvider {
    /// Builds a metadata graph for all root tables and their dependencies.
    pub async fn build_metadata_graph(
        introspector: &dyn SchemaIntrospector,
        tables: &[String],
    ) -> Result<HashMap<String, TableMetadata>, DriverError> {
        let mut graph = HashMap::new();
        let mut visited = HashSet::new();

        for table in tables {
            Self::build_metadata_graph_recursive(table, introspector, &mut graph, &mut visited)
                .await?;
        }

        Ok(graph)
    }

    pub fn construct_table_metadata(
        table: &str,
        columns: HashMap<String, ColumnMetadata>,
        fks: Vec<ForeignKeyMetadata>,
    ) -> Result<TableMetadata, DriverError> {
        let primary_keys: Vec<String> = columns
            .values()
            .filter(|col| col.is_primary_key)
            .map(|col| col.name.clone())
            .collect();

        Ok(TableMetadata {
            name: table.to_string(),
            schema: None,
            columns,
            primary_keys,
            foreign_keys: fks,
            referenced_tables: HashMap::new(),
            referencing_tables: HashMap::new(),
        })
    }

    fn build_metadata_graph_recursive<'a>(
        table_name: &'a str,
        introspector: &'a (dyn SchemaIntrospector + Send + Sync),
        graph: &'a mut HashMap<String, TableMetadata>,
        visited: &'a mut HashSet<String>,
    ) -> MetadataFuture<'a, TableMetadata> {
        Box::pin(async move {
            if let Some(metadata) = graph.get(table_name) {
                return Ok(metadata.clone());
            }

            if !visited.insert(table_name.to_string()) {
                return Err(DriverError::CircularReference(format!(
                    "Circular reference detected for table: {table_name}"
                )));
            }

            let mut metadata = introspector.table_metadata(table_name).await?;
            graph.insert(table_name.to_string(), metadata.clone());

            // Fetch forward and backward references
            Self::fetch_forward_references(table_name, &mut metadata, introspector, graph, visited)
                .await?;
            Self::fetch_backward_references(
                table_name,
                &mut metadata,
                introspector,
                graph,
                visited,
            )
            .await?;

            graph.insert(table_name.to_string(), metadata.clone());

            Ok(metadata)
        })
    }

    fn fetch_forward_references<'a>(
        table_name: &'a str,
        metadata: &'a mut TableMetadata,
        introspector: &'a (dyn SchemaIntrospector + Send + Sync),
        graph: &'a mut HashMap<String, TableMetadata>,
        visited: &'a mut HashSet<String>,
    ) -> MetadataFuture<'a, ()> {
        Box::pin(async move {
            for fk in &metadata.foreign_keys {
                let ref_table = &fk.referenced_table;
                let ref_metadata =
                    Self::build_metadata_graph_recursive(ref_table, introspector, graph, visited)
                        .await?;

                metadata
                    .referenced_tables
                    .insert(ref_table.clone(), ref_metadata.clone());

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
            Ok(())
        })
    }

    fn fetch_backward_references<'a>(
        table_name: &'a str,
        metadata: &'a mut TableMetadata,
        introspector: &'a (dyn SchemaIntrospector + Send + Sync),
        graph: &'a mut HashMap<String, TableMetadata>,
        visited: &'a mut HashSet<String>,
    ) -> MetadataFuture<'a, ()> {
        Box::pin(async move {
            let referencing_tables = introspector.referencing_tables(table_name).await?;

            for ref_table in referencing_tables {
                let ref_metadata =
                    Self::build_metadata_graph_recursive(&ref_table, introspector, graph, visited)
                        .await?;

                metadata
                    .referencing_tables
                    .insert(ref_table.clone(), ref_metadata.clone());

                graph
                    .entry(ref_table.clone())
                    .and_modify(|t| {
                        t.referenced_tables
                            .insert(table_name.to_string(), metadata.clone());
                    })
                    .or_insert_with(|| {
                        let mut t = ref_metadata.clone();
                        t.referenced_tables
                            .insert(table_name.to_string(), metadata.clone());
                        t
                    });
            }
            Ok(())
        })
    }
}
