use crate::adapter::PgAdapter;
use async_trait::async_trait;
use data_model::records::row_data::RowData;
use query_builder::dialect;
use sql_adapter::{
    adapter::SqlAdapter,
    destination::DbDataDestination,
    error::db::DbError,
    join::clause::JoinClause,
    metadata::{provider::MetadataHelper, table::TableMetadata},
    query::{column::ColumnDef, generator::QueryGenerator},
};
use std::{collections::HashMap, sync::Arc};
use tracing::info;

pub struct PgDestination {
    pub adapter: PgAdapter,

    /// The metadata for the primary source table
    pub primary_meta: Option<TableMetadata>,

    /// Metadata for any child tables (via FKs) when cascading
    related_meta: HashMap<String, TableMetadata>,

    dialect: dialect::Postgres,
}

impl PgDestination {
    pub fn new(adapter: PgAdapter) -> Self {
        Self {
            adapter,
            primary_meta: None,
            related_meta: HashMap::new(),
            dialect: dialect::Postgres,
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

        let num_rows = rows.len();
        let generator = QueryGenerator::new(&self.dialect);
        let (sql, params) = generator.insert_batch(meta, rows);

        info!("Inserting {} rows into {}", num_rows, meta.name);
        self.adapter.execute_with_params(&sql, params).await?;

        Ok(())
    }

    async fn toggle_trigger(&self, table: &str, enable: bool) -> Result<(), DbError> {
        let (sql, _params) = QueryGenerator::new(&self.dialect).toggle_triggers(table, enable);

        info!(
            "{} triggers on table {}",
            if enable { "Enabling" } else { "Disabling" },
            table
        );
        self.adapter.execute(&sql).await?;

        Ok(())
    }

    async fn table_exists(&self, table: &str) -> Result<bool, DbError> {
        self.adapter.table_exists(table).await
    }

    async fn add_column(&self, table: &str, column: &ColumnDef) -> Result<(), DbError> {
        let (sql, _params) = QueryGenerator::new(&self.dialect).add_column(table, column.clone());

        info!("Adding column {} to table {}", column.name, table);
        self.adapter.execute(&sql).await?;

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
                    .is_none_or(|p| !p.name.eq_ignore_ascii_case(&meta.name))
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
