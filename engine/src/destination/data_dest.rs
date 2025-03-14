use std::{any::Any, sync::Arc};

use crate::source::record::DataRecord;
use async_trait::async_trait;
use smql::statements::connection::DataFormat;
use sql_adapter::{adapter::DbAdapter, metadata::table::TableMetadata};

use super::postgres::PgDestination;

#[async_trait]
pub trait DataDestination: Send + Sync + Any {
    type Record: DataRecord + Send + Sync + Sized + 'static;

    fn as_any(&self) -> &dyn std::any::Any;

    async fn write(&self, data: Vec<Self::Record>) -> Result<(), Box<dyn std::error::Error>>;
}

#[async_trait]
pub trait DbDataDestination: DataDestination {
    async fn infer_schema(
        &self,
        metadata: &TableMetadata,
    ) -> Result<(), Box<dyn std::error::Error>>;
}

pub async fn create_data_destination(
    data_format: DataFormat,
    adapter: Box<dyn DbAdapter + Send + Sync>,
) -> Result<
    Arc<dyn DataDestination<Record = Box<dyn DataRecord + Send + Sync>>>,
    Box<dyn std::error::Error>,
> {
    match data_format {
        DataFormat::Postgres => {
            let destination = PgDestination::new(adapter)?;
            Ok(Arc::new(destination))
        }
        DataFormat::MySql => unimplemented!("MySql data destination not implemented"),
        _ => unimplemented!("Unsupported data destination"),
    }
}
