use super::{column::ColumnMetadata, foreign_key::ForeignKeyMetadata};
use crate::database::{
    managers::{base::DbManager, mysql::MySqlManager},
    query::loader::QueryLoader,
};
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
    pub fn build_graph<'a>(
        table_name: &'a str,
        manager: &'a MySqlManager,
        graph: &'a mut HashMap<String, TableMetadata>,
        visited: &'a mut HashSet<String>,
    ) -> Pin<Box<dyn Future<Output = Result<TableMetadata, sqlx::Error>> + 'a>> {
        Box::pin(async move {
            if let Some(metadata) = graph.get(table_name) {
                return Ok(metadata.clone());
            }

            if !visited.insert(table_name.to_string()) {
                return Err(sqlx::Error::Protocol(format!(
                    "Circular dependency detected for table: {}",
                    table_name
                )));
            }

            let mut metadata = Self::from_table(table_name, manager).await?;
            graph.insert(table_name.to_string(), metadata.clone());

            for fk in &metadata.foreign_keys {
                let ref_table = &fk.referenced_table;

                if !graph.contains_key(ref_table) {
                    let ref_metadata =
                        Self::build_graph(ref_table, manager, graph, visited).await?;

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

    pub async fn from_table(table: &str, manager: &MySqlManager) -> Result<Self, sqlx::Error> {
        let query = QueryLoader::table_metadata_query()
            .map_err(|_| sqlx::Error::Configuration("Table metadata query not found".into()))?;

        let rows = sqlx::query(&query)
            .bind(table)
            .bind(table)
            .bind(table)
            .bind(table)
            .fetch_all(manager.pool())
            .await?;

        let columns: HashMap<String, ColumnMetadata> = rows
            .iter()
            .map(|row| ColumnMetadata::from(row))
            .map(|col| (col.name.clone(), col))
            .collect();

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

        Ok(Self {
            name: table.to_string(),
            schema: None,
            columns,
            primary_keys,
            foreign_keys,
            referenced_tables: HashMap::new(),
            referencing_tables: HashMap::new(),
        })
    }
}
