use super::{providers::mysql::MySqlDataSource, record::DataRecord};
use async_trait::async_trait;
use smql::statements::connection::DataFormat;
use sql_adapter::{adapter::DbAdapter, metadata::table::TableMetadata};
use std::{any::Any, sync::Arc};

#[async_trait]
pub trait DataSource: Send + Sync + Any {
    type Record: DataRecord + Send + Sync + 'static;

    fn as_any(&self) -> &dyn std::any::Any;

    async fn fetch_data(
        &self,
        batch_size: usize,
        offset: Option<usize>,
    ) -> Result<Vec<Self::Record>, Box<dyn std::error::Error>>;
}

#[async_trait]
pub trait DbDataSource: DataSource {
    async fn get_metadata(&self) -> Result<TableMetadata, Box<dyn std::error::Error>>;
}

pub async fn create_data_source(
    source: String,
    data_format: DataFormat,
    adapter: Box<dyn DbAdapter + Send + Sync>,
) -> Result<
    Arc<dyn DataSource<Record = Box<dyn DataRecord + Send + Sync>>>,
    Box<dyn std::error::Error>,
> {
    match data_format {
        DataFormat::MySql => {
            let source = MySqlDataSource::new(&source, adapter).await?;
            Ok(Arc::new(source))
        }
        DataFormat::Postgres => unimplemented!("Postgres data source not implemented"),
        _ => unimplemented!("Unsupported data source"),
    }
}
