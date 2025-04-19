use super::providers::postgres::PgDestination;
use crate::{adapter::Adapter, record::Record};
use async_trait::async_trait;
use smql::statements::connection::DataFormat;
use sql_adapter::{
    adapter::SqlAdapter, metadata::table::TableMetadata, query::column::ColumnDef,
    schema::plan::SchemaPlan,
};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;

#[derive(Clone)]
pub enum DataDestination {
    Database(Arc<Mutex<dyn DbDataDestination>>),
}

impl DataDestination {
    pub fn from_adapter(
        format: DataFormat,
        adapter: Adapter,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        match format {
            DataFormat::Postgres => match adapter {
                Adapter::Postgres(adapter) => {
                    let destination = PgDestination::new(adapter);
                    Ok(DataDestination::Database(Arc::new(Mutex::new(destination))))
                }
                _ => Err("Expected Postgres adapter, but got a different type".into()),
            },
            DataFormat::MySql => {
                // Add once implemented
                Err("MySql data destination is not implemented yet".into())
            }
            other => Err(format!("Unsupported data source format: {:?}", other).into()),
        }
    }
}

#[async_trait]
pub trait DbDataDestination: Send + Sync {
    async fn write_batch(
        &self,
        metadata: &TableMetadata,
        records: Vec<Record>,
    ) -> Result<(), Box<dyn std::error::Error>>;
    async fn infer_schema(
        &self,
        schema_plan: &SchemaPlan<'_>,
    ) -> Result<(), Box<dyn std::error::Error>>;
    async fn toggle_trigger(
        &self,
        table: &str,
        enable: bool,
    ) -> Result<(), Box<dyn std::error::Error>>;
    async fn table_exists(&self, table: &str) -> Result<bool, Box<dyn std::error::Error>>;
    async fn add_column(
        &self,
        table: &str,
        column: &ColumnDef,
    ) -> Result<(), Box<dyn std::error::Error>>;

    fn get_metadata(&self, table: &str) -> &TableMetadata;
    fn set_metadata(&mut self, metadata: HashMap<String, TableMetadata>);

    fn get_tables(&self) -> Vec<TableMetadata>;

    fn adapter(&self) -> Arc<(dyn SqlAdapter + Send + Sync)>;
}
