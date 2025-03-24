use super::{
    column::{data_type::ColumnDataType, metadata::ColumnMetadata},
    foreign_key::ForeignKeyMetadata,
    table::TableMetadata,
};
use crate::{adapter::SqlAdapter, query::builder::SqlQueryBuilder};
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
};

// MetadataFuture is a type alias for a Future that returns a Result
// containing the TableMetadata or an error
pub type MetadataFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, Box<dyn std::error::Error>>> + 'a>>;

pub struct MetadataProvider;

impl MetadataProvider {
    pub async fn build_table_metadata(
        adapter: &(dyn SqlAdapter + Send + Sync),
        table: &str,
    ) -> Result<TableMetadata, Box<dyn std::error::Error>> {
        let mut graph = HashMap::new();
        let mut visited = HashSet::new();
        let metadata =
            Self::build_metadata_dep_graph(table, adapter, &mut graph, &mut visited).await?;
        Ok(metadata)
    }

    pub fn process_metadata_cols(
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

    pub fn resolve_insert_order(metadata: &TableMetadata) -> Vec<&TableMetadata> {
        let mut visited = HashSet::new();
        let mut order = Vec::new();

        fn visit<'a>(
            meta: &'a TableMetadata,
            visited: &mut HashSet<&'a str>,
            order: &mut Vec<&'a TableMetadata>,
            referenced_tables: &'a HashMap<String, TableMetadata>,
        ) {
            if !visited.insert(&meta.name) {
                return;
            }

            for fk in &meta.foreign_keys {
                if let Some(ref_meta) = referenced_tables.get(&fk.referenced_table) {
                    visit(ref_meta, visited, order, &ref_meta.referenced_tables);
                }
            }

            order.push(meta);
        }

        visit(
            metadata,
            &mut visited,
            &mut order,
            &metadata.referenced_tables,
        );

        order
    }

    pub fn collect_schema_deps<F, T>(
        metadata: &TableMetadata,
        type_converter: &F,
        custom_type_extractor: &T,
    ) -> (HashSet<String>, HashSet<String>, HashSet<(String, String)>)
    where
        F: Fn(&ColumnMetadata) -> String,
        T: Fn(&TableMetadata) -> Vec<&ColumnMetadata>,
    {
        fn visit<F, T>(
            metadata: &TableMetadata,
            type_converter: &F,
            custom_type_extractor: &T,
            visited: &mut HashSet<String>,
            table_queries: &mut HashSet<String>,
            constraint_queries: &mut HashSet<String>,
            enum_declarations: &mut HashSet<(String, String)>,
        ) where
            F: Fn(&ColumnMetadata) -> String,
            T: Fn(&TableMetadata) -> Vec<&ColumnMetadata>,
        {
            if !visited.insert(metadata.name.clone()) {
                return;
            }

            metadata
                .referenced_tables
                .values()
                .chain(metadata.referencing_tables.values())
                .for_each(|related_table| {
                    visit(
                        related_table,
                        type_converter,
                        custom_type_extractor,
                        visited,
                        table_queries,
                        constraint_queries,
                        enum_declarations,
                    );
                });

            let columns = metadata.to_column_definitions(type_converter);
            table_queries.insert(
                SqlQueryBuilder::new()
                    .create_table(&metadata.name, &columns, &[])
                    .build()
                    .0,
            );

            for fk in metadata.to_fk_definitions() {
                constraint_queries.insert(
                    SqlQueryBuilder::new()
                        .add_foreign_key(&metadata.name, &fk)
                        .build()
                        .0,
                );
            }

            let custom_columns = custom_type_extractor(&metadata);
            custom_columns.iter().for_each(|c| {
                enum_declarations.insert((metadata.name.to_string(), c.name.clone()));
            })
        }

        let mut visited = HashSet::new();
        let mut table_queries = HashSet::new();
        let mut constraint_queries = HashSet::new();
        let mut enum_declarations = HashSet::new();

        visit(
            metadata,
            type_converter,
            custom_type_extractor,
            &mut visited,
            &mut table_queries,
            &mut constraint_queries,
            &mut enum_declarations,
        );

        (table_queries, constraint_queries, enum_declarations)
    }

    fn build_metadata_dep_graph<'a>(
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
                    Self::build_metadata_dep_graph(ref_table, adapter, graph, visited).await?;

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
                    Self::build_metadata_dep_graph(&ref_table, adapter, graph, visited).await?;

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
