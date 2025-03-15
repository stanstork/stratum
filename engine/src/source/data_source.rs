use super::providers::mysql::MySqlDataSource;
use crate::record::DataRecord;
use async_trait::async_trait;
use smql::{plan::MigrationPlan, statements::connection::DataFormat};
use sql_adapter::{adapter::DbAdapter, metadata::table::TableMetadata};
use std::sync::Arc;

pub enum DataSource {
    Database(Arc<dyn DbDataSource<Record = Box<dyn DataRecord + Send + Sync>>>),
}

#[async_trait]
pub trait DbDataSource: Send + Sync {
    type Record: DataRecord + Send + Sync + 'static;

    async fn fetch_data(
        &self,
        batch_size: usize,
        offset: Option<usize>,
    ) -> Result<Vec<Self::Record>, Box<dyn std::error::Error>>;
    async fn get_metadata(&self) -> Result<TableMetadata, Box<dyn std::error::Error>>;
}

pub async fn create_data_source(
    plan: &MigrationPlan,
    adapter: Box<dyn DbAdapter + Send + Sync>,
) -> Result<
    Arc<dyn DbDataSource<Record = Box<dyn DataRecord + Send + Sync>>>,
    Box<dyn std::error::Error>,
> {
    let source = plan.migration.source.first().unwrap();
    let data_format = plan.connections.source.data_format;

    match data_format {
        DataFormat::MySql => {
            let source = MySqlDataSource::new(&source, adapter).await?;
            Ok(Arc::new(source))
        }
        DataFormat::Postgres => unimplemented!("Postgres data source not implemented"),
        _ => unimplemented!("Unsupported data source"),
    }
}
