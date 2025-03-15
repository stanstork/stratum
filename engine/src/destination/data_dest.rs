use super::providers::postgres::PgDestination;
use crate::record::DataRecord;
use async_trait::async_trait;
use smql::{plan::MigrationPlan, statements::connection::DataFormat};
use sql_adapter::{adapter::DbAdapter, metadata::table::TableMetadata};
use std::sync::Arc;

pub enum DataDestination {
    Database(Arc<dyn DbDataDestination<Record = Box<dyn DataRecord + Send + Sync>>>),
}

#[async_trait]
pub trait DbDataDestination: Send + Sync {
    type Record: DataRecord + Send + Sync + Sized + 'static;

    async fn write(&self, data: Vec<Self::Record>) -> Result<(), Box<dyn std::error::Error>>;
    async fn infer_schema(
        &self,
        metadata: &TableMetadata,
    ) -> Result<(), Box<dyn std::error::Error>>;
}

pub async fn create_data_destination(
    plan: &MigrationPlan,
    adapter: Box<dyn DbAdapter + Send + Sync>,
) -> Result<
    Arc<dyn DbDataDestination<Record = Box<dyn DataRecord + Send + Sync>>>,
    Box<dyn std::error::Error>,
> {
    let data_format = plan.connections.destination.data_format;

    match data_format {
        DataFormat::Postgres => {
            let destination = PgDestination::new(adapter)?;
            Ok(Arc::new(destination))
        }
        DataFormat::MySql => unimplemented!("MySql data destination not implemented"),
        _ => unimplemented!("Unsupported data destination"),
    }
}
