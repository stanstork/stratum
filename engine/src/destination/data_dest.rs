use crate::source::record::DataRecord;
use async_trait::async_trait;
use smql::statements::connection::DataFormat;
use sql_adapter::adapter::DbAdapter;

use super::postgres::PgDestination;

#[async_trait]
pub trait DataDestination {
    type Record: DataRecord + Send + Sync + 'static;

    async fn write(&self, data: Vec<Self::Record>) -> Result<(), Box<dyn std::error::Error>>;
}

pub async fn create_data_destination(
    data_format: DataFormat,
    adapter: Box<dyn DbAdapter + Send + Sync>,
) -> Result<
    Box<dyn DataDestination<Record = Box<dyn DataRecord + Send + Sync>>>,
    Box<dyn std::error::Error>,
> {
    match data_format {
        DataFormat::Postgres => {
            let destination = PgDestination::new(adapter)?;
            Ok(Box::new(destination))
        }
        DataFormat::MySql => unimplemented!("MySql data destination not implemented"),
        _ => unimplemented!("Unsupported data destination"),
    }
}
