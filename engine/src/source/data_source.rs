use super::providers::mysql::MySqlDataSource;
use crate::{adapter::Adapter, record::Record};
use async_trait::async_trait;
use smql::{plan::MigrationPlan, statements::connection::DataFormat};
use sql_adapter::adapter::SqlAdapter;
use sql_adapter::metadata::table::TableMetadata;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub enum DataSource {
    Database(Arc<Mutex<dyn DbDataSource>>),
}

impl DataSource {
    pub async fn source_name(&self) -> String {
        match self {
            DataSource::Database(source) => source.lock().await.table_name().to_string(),
        }
    }
}

#[async_trait]
pub trait DbDataSource: Send + Sync {
    async fn fetch_data(
        &self,
        batch_size: usize,
        offset: Option<usize>,
    ) -> Result<Vec<Record>, Box<dyn std::error::Error>>;

    fn get_metadata(&self) -> &TableMetadata;
    fn set_metadata(&mut self, metadata: TableMetadata);
    fn table_name(&self) -> &str;
    fn adapter(&self) -> &(dyn SqlAdapter + Send + Sync);
}

pub async fn create_data_source(
    plan: &MigrationPlan,
    adapter: Adapter,
) -> Result<Arc<Mutex<dyn DbDataSource>>, Box<dyn std::error::Error>> {
    let source = plan.migration.source.first().unwrap();
    let data_format = plan.connections.source.data_format;

    match data_format {
        DataFormat::MySql => {
            if let Adapter::MySql(adapter) = adapter {
                let source = MySqlDataSource::new(source, adapter).await?;
                Ok(Arc::new(Mutex::new(source)))
            } else {
                panic!("Invalid adapter type")
            }
        }
        DataFormat::Postgres => unimplemented!("Postgres data source not implemented"),
        _ => unimplemented!("Unsupported data source"),
    }
}
