use crate::{destination::data_dest::DbDataDestination, record::Record};
use async_trait::async_trait;
use postgres::{data_type::ColumnDataTypeMapper, postgres::PgAdapter};
use sql_adapter::{
    adapter::SqlAdapter,
    metadata::{provider::MetadataProvider, table::TableMetadata},
    query::builder::{ColumnInfo, ForeignKeyInfo, SqlQueryBuilder},
};
use std::collections::HashSet;
use tracing::{error, info};

pub struct PgDestination {
    metadata: Option<TableMetadata>,
    adapter: PgAdapter,
}

impl PgDestination {
    pub async fn new(adapter: PgAdapter, table: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let metadata = if adapter.table_exists(table).await? {
            Some(MetadataProvider::build_table_metadata(&adapter, table).await?)
        } else {
            None
        };

        Ok(PgDestination { adapter, metadata })
    }

    pub async fn table_exists(&self, table: &str) -> Result<bool, Box<dyn std::error::Error>> {
        self.adapter.table_exists(table).await.map_err(Into::into)
    }
}

#[async_trait]
impl DbDataDestination for PgDestination {
    async fn infer_schema(
        &self,
        metadata: &TableMetadata,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if self.table_exists(&metadata.name).await? {
            info!("Table '{}' already exists", metadata.name);
            return Ok(());
        }

        let mut visited = HashSet::new();
        let mut queries = Vec::new();

        // Collect tables recursively and ensure correct creation order
        self.collect_ref_tables(metadata, &mut visited, &mut queries);

        for query in queries {
            let sql = query.build().0;
            info!("Executing query: {}", sql);

            if let Err(err) = self.adapter.execute(&sql).await {
                error!("Failed to execute query: {}\nError: {:?}", sql, err);
                return Err(err);
            }
        }

        info!("Schema inference completed");
        Ok(())
    }

    async fn write(
        &self,
        metadata: &TableMetadata,
        record: Record,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let columns = match record {
            Record::RowData(row) => row
                .columns
                .iter()
                .map(|col| {
                    let value = col
                        .value
                        .clone()
                        .map_or("NULL".to_string(), |val| val.to_string());
                    (col.name.clone(), value)
                })
                .collect::<Vec<(String, String)>>(),
            _ => return Err("Invalid record type".into()),
        };

        let query = SqlQueryBuilder::new()
            .insert_into(&metadata.name, &columns)
            .build();

        info!("Executing query: {}", query.0);
        self.adapter.execute(&query.0).await?;

        Ok(())
    }

    async fn write_batch(
        &self,
        metadata: &TableMetadata,
        records: Vec<Record>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if records.is_empty() {
            return Ok(());
        }

        let mut columns = Vec::new();
        let mut values = Vec::new();

        for record in records {
            let row = match record {
                Record::RowData(row) => row,
                _ => return Err("Invalid record type".into()),
            };

            if columns.is_empty() {
                columns = row.columns.iter().map(|col| col.name.clone()).collect();
            }

            let row_values = row
                .columns
                .iter()
                .map(|col| {
                    col.value
                        .clone()
                        .map_or("NULL".to_string(), |val| val.to_string())
                })
                .collect::<Vec<String>>();

            values.push(row_values);
        }

        if columns.is_empty() {
            return Err("No valid records found".into());
        }

        let query = SqlQueryBuilder::new()
            .insert_batch(&metadata.name, columns, values)
            .build();

        info!("Executing query: {}", query.0);
        self.adapter.execute(&query.0).await?;

        Ok(())
    }

    fn adapter(&self) -> Box<dyn SqlAdapter + Send + Sync> {
        Box::new(self.adapter.clone())
    }

    fn set_metadata(&mut self, metadata: TableMetadata) {
        self.metadata = Some(metadata);
    }

    fn metadata(&self) -> &TableMetadata {
        self.metadata.as_ref().expect("Metadata not set")
    }
}

impl PgDestination {
    fn collect_ref_tables(
        &self,
        table: &TableMetadata,
        visited: &mut HashSet<String>,
        queries: &mut Vec<SqlQueryBuilder>,
    ) {
        if visited.contains(&table.name) {
            return;
        }

        for referenced_table in table.referenced_tables.values() {
            self.collect_ref_tables(referenced_table, visited, queries);
        }

        let columns = self.get_columns(table);
        let foreign_keys = self.get_foreign_keys(table);
        let query_builder =
            SqlQueryBuilder::new().create_table(&table.name, &columns, &foreign_keys);

        queries.push(query_builder);
        visited.insert(table.name.clone());
    }

    fn get_columns(&self, metadata: &TableMetadata) -> Vec<ColumnInfo> {
        metadata
            .columns
            .iter()
            .map(|(name, col)| ColumnInfo {
                name: name.clone(),
                data_type: col.data_type.to_pg_string(),
                is_nullable: col.is_nullable,
                is_primary_key: metadata.primary_keys.contains(name),
                default: col.default_value.as_ref().map(ToString::to_string),
            })
            .collect::<Vec<_>>()
    }

    fn get_foreign_keys(&self, metadata: &TableMetadata) -> Vec<ForeignKeyInfo> {
        metadata
            .foreign_keys
            .iter()
            .map(|fk| ForeignKeyInfo {
                column: fk.column.clone(),
                referenced_table: fk.referenced_table.clone(),
                referenced_column: fk.referenced_column.clone(),
            })
            .collect::<Vec<_>>()
    }
}
