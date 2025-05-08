use crate::adapter::PgAdapter;
use async_trait::async_trait;
use sql_adapter::{
    adapter::SqlAdapter,
    destination::DbDataDestination,
    error::db::DbError,
    metadata::{provider::MetadataHelper, table::TableMetadata},
    query::{builder::SqlQueryBuilder, column::ColumnDef},
    row::row_data::RowData,
    schema::plan::SchemaPlan,
};
use std::{collections::HashMap, sync::Arc};
use tracing::{error, info};

pub struct PgDestination {
    pub metadata: HashMap<String, TableMetadata>,
    pub adapter: PgAdapter,
}

impl PgDestination {
    pub fn new(adapter: PgAdapter) -> Self {
        Self {
            metadata: HashMap::new(),
            adapter,
        }
    }
}

#[async_trait]
impl DbDataDestination for PgDestination {
    type Error = DbError;

    async fn write_batch(&self, meta: &TableMetadata, rows: Vec<RowData>) -> Result<(), DbError> {
        if rows.is_empty() {
            return Ok(());
        }

        let columns = meta
            .columns
            .values()
            .map(ColumnDef::new)
            .collect::<Vec<_>>();

        if columns.is_empty() {
            return Err(DbError::Write("No columns found in metadata".to_string()));
        }

        let all_values: Vec<Vec<String>> = rows
            .into_iter()
            .map(|row| {
                columns
                    .iter()
                    .map(|col| {
                        row.columns
                            .iter()
                            .find(|rc| rc.name.eq_ignore_ascii_case(&col.name))
                            .and_then(|rc| rc.value.clone())
                            .map_or_else(|| "NULL".to_string(), |val| val.to_string())
                    })
                    .collect()
            })
            .collect();

        let query = SqlQueryBuilder::new()
            .insert_batch(&meta.name, columns, all_values)
            .build();

        info!("Executing insert into `{}`", meta.name);
        self.adapter.execute(&query.0).await?;

        Ok(())
    }

    async fn infer_schema(&self, schema_plan: &SchemaPlan<'_>) -> Result<(), DbError> {
        let enum_queries = schema_plan.enum_queries().await?;
        let table_queries = schema_plan.table_queries().await;
        let fk_queries = schema_plan.fk_queries();

        let all_queries = enum_queries
            .iter()
            .chain(&table_queries)
            .chain(&fk_queries)
            .cloned();

        for query in all_queries {
            info!("Executing query: {}", query);
            if let Err(err) = self.adapter.execute(&query).await {
                error!("Failed to execute query: {}\nError: {:?}", query, err);
                return Err(err);
            }
        }

        info!("Schema inference completed");
        Ok(())
    }

    async fn toggle_trigger(&self, table: &str, enable: bool) -> Result<(), DbError> {
        let query = SqlQueryBuilder::new().toggle_trigger(table, enable).build();

        info!("Executing query: {}", query.0);
        self.adapter.execute(&query.0).await?;

        Ok(())
    }

    async fn table_exists(&self, table: &str) -> Result<bool, DbError> {
        self.adapter.table_exists(table).await
    }

    async fn add_column(&self, table: &str, column: &ColumnDef) -> Result<(), DbError> {
        let query = SqlQueryBuilder::new().add_column(table, column).build();

        info!("Executing query: {}", query.0);
        self.adapter.execute(&query.0).await?;

        Ok(())
    }
}

impl MetadataHelper for PgDestination {
    fn get_metadata(&self, table: &str) -> &TableMetadata {
        self.metadata
            .get(table)
            .unwrap_or_else(|| panic!("Metadata for table {} not found", table))
    }

    fn set_metadata(&mut self, metadata: HashMap<String, TableMetadata>) {
        self.metadata = metadata;
    }

    fn get_tables(&self) -> Vec<TableMetadata> {
        self.metadata.values().cloned().collect()
    }

    fn adapter(&self) -> Arc<(dyn SqlAdapter + Send + Sync)> {
        Arc::new(self.adapter.clone())
    }
}
