use super::providers::postgres::PgDestination;
use crate::{adapter::Adapter, record::DataRecord};
use async_trait::async_trait;
use smql::{plan::MigrationPlan, statements::connection::DataFormat};
use sql_adapter::{adapter::DbAdapter, metadata::table::TableMetadata};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub enum DataDestination {
    Database(Arc<Mutex<dyn DbDataDestination<Record = Box<dyn DataRecord + Send + Sync>>>>),
}

#[async_trait]
pub trait DbDataDestination: Send + Sync {
    type Record: DataRecord + Send + Sync + Sized + 'static;

    async fn write(&self, data: Vec<Self::Record>) -> Result<(), Box<dyn std::error::Error>>;
    async fn infer_schema(
        &self,
        metadata: &TableMetadata,
    ) -> Result<(), Box<dyn std::error::Error>>;
    async fn validate_schema(
        &self,
        metadata: &TableMetadata,
    ) -> Result<(), Box<dyn std::error::Error>>;

    fn adapter(&self) -> Box<dyn DbAdapter + Send + Sync>;
    fn set_metadata(&mut self, metadata: TableMetadata);
    fn metadata(&self) -> &TableMetadata;
}

pub async fn create_data_destination(
    plan: &MigrationPlan,
    adapter: Adapter,
) -> Result<
    Arc<Mutex<dyn DbDataDestination<Record = Box<dyn DataRecord + Send + Sync>>>>,
    Box<dyn std::error::Error>,
> {
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
