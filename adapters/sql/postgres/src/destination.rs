use crate::adapter::PgAdapter;
use async_trait::async_trait;
use common::row_data::RowData;
use sql_adapter::{
    adapter::SqlAdapter,
    destination::DbDataDestination,
    error::db::DbError,
    join::clause::JoinClause,
    metadata::{provider::MetadataHelper, table::TableMetadata},
    query::{builder::SqlQueryBuilder, column::ColumnDef},
};
use std::{collections::HashMap, sync::Arc};
use tracing::info;

pub struct PgDestination {
    pub adapter: PgAdapter,

    /// The metadata for the primary source table
    pub primary_meta: Option<TableMetadata>,

    /// Metadata for any child tables (via FKs) when cascading
    related_meta: HashMap<String, TableMetadata>,
}

impl PgDestination {
    pub fn new(adapter: PgAdapter) -> Self {
        Self {
            adapter,
            primary_meta: None,
            related_meta: HashMap::new(),
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
                        row.field_values
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
    fn get_metadata(&self) -> &Option<TableMetadata> {
        &self.primary_meta
    }

    fn set_metadata(&mut self, meta: TableMetadata) {
        self.primary_meta = Some(meta);
    }

    fn tables(&self) -> Vec<TableMetadata> {
        // pull out the primary table metadata
        let primary = self
            .primary_meta
            .as_ref()
            .map(|meta| vec![meta.clone()])
            .unwrap_or_default();

        // include related tables
        let related = self
            .related_meta
            .values()
            .filter(|meta| {
                self.primary_meta
                    .as_ref()
                    .map_or(true, |p| !p.name.eq_ignore_ascii_case(&meta.name))
            })
            .cloned()
            .collect::<Vec<_>>();

        primary.into_iter().chain(related).collect()
    }

    fn adapter(&self) -> Arc<(dyn SqlAdapter + Send + Sync)> {
        Arc::new(self.adapter.clone())
    }

    fn set_related_meta(&mut self, meta: HashMap<String, TableMetadata>) {
        self.related_meta = meta;
    }

    fn set_cascade_joins(&mut self, _table: String, _joins: Vec<JoinClause>) {
        // No-op for now
    }
}
