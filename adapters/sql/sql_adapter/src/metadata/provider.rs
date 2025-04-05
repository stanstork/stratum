use super::{column::metadata::ColumnMetadata, fk::ForeignKeyMetadata, table::TableMetadata};
use crate::{adapter::SqlAdapter, schema::plan::SchemaPlan};
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
};

// MetadataFuture is a type alias for a Future that returns a Result
// containing the TableMetadata or an error
pub type MetadataFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, Box<dyn std::error::Error>>> + Send + 'a>>;

pub struct MetadataProvider;

impl MetadataProvider {
    /// Builds a metadata graph for all root tables and their dependencies.
    pub async fn build_metadata_graph(
        adapter: &(dyn SqlAdapter + Send + Sync),
        tables: &[String],
    ) -> Result<HashMap<String, TableMetadata>, Box<dyn std::error::Error>> {
        let mut graph = HashMap::new();
        let mut visited = HashSet::new();

        for table in tables {
            Self::build_metadata_graph_recursive(table, adapter, &mut graph, &mut visited).await?;
        }

        Ok(graph)
    }

    pub fn construct_table_metadata(
        table: &str,
        columns: HashMap<String, ColumnMetadata>,
    ) -> Result<TableMetadata, Box<dyn std::error::Error>> {
        let primary_keys: Vec<String> = columns
            .values()
            .filter(|col| col.is_primary_key)
            .map(|col| col.name.clone())
            .collect();

        let foreign_keys: Vec<ForeignKeyMetadata> = columns
            .values()
            .filter_map(|col| {
                col.referenced_table
                    .as_ref()
                    .zip(col.referenced_column.as_ref())
                    .map(|(ref_table, ref_column)| ForeignKeyMetadata {
                        column: col.name.clone(),
                        referenced_table: ref_table.clone(),
                        referenced_column: ref_column.clone(),
                        nullable: col.is_nullable,
                    })
            })
            .collect();

        Ok(TableMetadata {
            name: table.to_string(),
            schema: None,
            columns,
            primary_keys,
            foreign_keys,
            referenced_tables: HashMap::new(),
            referencing_tables: HashMap::new(),
        })
    }

    pub fn collect_schema_deps(metadata: &TableMetadata, plan: &mut SchemaPlan) {
        let mut visited = HashSet::new();
        Self::visit_schema_deps(metadata, plan, &mut visited);
    }

    fn build_metadata_graph_recursive<'a>(
        table_name: &'a str,
        adapter: &'a (dyn SqlAdapter + Send + Sync),
        graph: &'a mut HashMap<String, TableMetadata>,
        visited: &'a mut HashSet<String>,
    ) -> MetadataFuture<'a, TableMetadata> {
        Box::pin(async move {
            if let Some(metadata) = graph.get(table_name) {
                return Ok(metadata.clone());
            }

            if !visited.insert(table_name.to_string()) {
                return Err("Circular reference detected".into());
            }

            let mut metadata = adapter.fetch_metadata(table_name).await?;
            graph.insert(table_name.to_string(), metadata.clone());

            // Fetch forward and backward references
            Self::fetch_forward_references(table_name, &mut metadata, adapter, graph, visited)
                .await?;
            Self::fetch_backward_references(table_name, &mut metadata, adapter, graph, visited)
                .await?;

            graph.insert(table_name.to_string(), metadata.clone());

            Ok(metadata)
        })
    }

    fn fetch_forward_references<'a>(
        table_name: &'a str,
        metadata: &'a mut TableMetadata,
        adapter: &'a (dyn SqlAdapter + Send + Sync),
        graph: &'a mut HashMap<String, TableMetadata>,
        visited: &'a mut HashSet<String>,
    ) -> MetadataFuture<'a, ()> {
        Box::pin(async move {
            for fk in &metadata.foreign_keys {
                let ref_table = &fk.referenced_table;
                let ref_metadata =
                    Self::build_metadata_graph_recursive(ref_table, adapter, graph, visited)
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
        adapter: &'a (dyn SqlAdapter + Send + Sync),
        graph: &'a mut HashMap<String, TableMetadata>,
        visited: &'a mut HashSet<String>,
    ) -> MetadataFuture<'a, ()> {
        Box::pin(async move {
            let referencing_tables = adapter.fetch_referencing_tables(table_name).await?;

            for ref_table in referencing_tables {
                let ref_metadata =
                    Self::build_metadata_graph_recursive(&ref_table, adapter, graph, visited)
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

    fn visit_schema_deps(
        metadata: &TableMetadata,
        plan: &mut SchemaPlan<'_>,
        visited: &mut HashSet<String>,
    ) {
        if !visited.insert(metadata.name.clone()) || plan.metadata_exists(&metadata.name) {
            return;
        }

        metadata
            .referenced_tables
            .values()
            .chain(metadata.referencing_tables.values())
            .for_each(|related| {
                Self::visit_schema_deps(related, plan, visited);
            });

        plan.add_column_defs(&metadata.name, metadata.column_defs(plan.type_converter()));
        plan.add_fk_defs(&metadata.name, metadata.fk_defs());

        for col in (plan.type_extractor())(metadata) {
            plan.add_enum_def(&metadata.name, &col.name);
        }
    }
}
