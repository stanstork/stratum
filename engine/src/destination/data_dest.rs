use super::providers::postgres::PgDestination;
use crate::{adapter::Adapter, record::Record};
use async_trait::async_trait;
use smql::{plan::MigrationPlan, statements::connection::DataFormat};
use sql_adapter::{adapter::SqlAdapter, metadata::table::TableMetadata, schema::plan::SchemaPlan};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub enum DataDestination {
    Database(Arc<Mutex<dyn DbDataDestination>>),
}

#[async_trait]
pub trait DbDataDestination: Send + Sync {
    async fn write(
        &self,
        metadata: &TableMetadata,
        record: Record,
    ) -> Result<(), Box<dyn std::error::Error>>;
    async fn write_batch(
        &self,
        metadata: &TableMetadata,
        records: Vec<Record>,
    ) -> Result<(), Box<dyn std::error::Error>>;
    async fn infer_schema(
        &self,
        schema_plan: &SchemaPlan,
    ) -> Result<(), Box<dyn std::error::Error>>;

    async fn toggle_trigger(
        &self,
        table: &str,
        enable: bool,
    ) -> Result<(), Box<dyn std::error::Error>>;

    async fn table_exists(&self, table: &str) -> Result<bool, Box<dyn std::error::Error>>;

    fn adapter(&self) -> &(dyn SqlAdapter + Send + Sync);
    fn set_metadata(&mut self, metadata: TableMetadata);
    fn metadata(&self) -> &TableMetadata;
}

pub async fn create_data_destination(
    plan: &MigrationPlan,
    adapter: Adapter,
) -> Result<Arc<Mutex<dyn DbDataDestination>>, Box<dyn std::error::Error>> {
    let data_format = plan.connections.destination.data_format;

    match data_format {
        DataFormat::Postgres => {
            if let Adapter::Postgres(adapter) = adapter {
                let destination = PgDestination::new(adapter, &plan.migration.target).await?;
                Ok(Arc::new(Mutex::new(destination)))
            } else {
                panic!("Invalid adapter type")
            }
        }
        DataFormat::MySql => unimplemented!("MySql data destination not implemented"),
        _ => unimplemented!("Unsupported data destination"),
    }
}
