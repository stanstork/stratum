use super::providers::mysql::MySqlDataSource;
use crate::{adapter::Adapter, record::Record};
use async_trait::async_trait;
use smql::{plan::MigrationPlan, statements::connection::DataFormat};
use sql_adapter::metadata::table::TableMetadata;
use std::sync::Arc;

#[derive(Clone)]
pub enum DataSource {
    Database(Arc<dyn DbDataSource>),
}

impl DataSource {
    pub fn source_name(&self) -> &str {
        match self {
            DataSource::Database(source) => source.table_name(),
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
    async fn get_metadata(&self) -> Result<TableMetadata, Box<dyn std::error::Error>>;
    fn table_name(&self) -> &str;
}

pub async fn create_data_source(
    plan: &MigrationPlan,
    adapter: Adapter,
) -> Result<Arc<dyn DbDataSource>, Box<dyn std::error::Error>> {
    let source = plan.migration.source.first().unwrap();
    let data_format = plan.connections.source.data_format;

    match data_format {
        DataFormat::MySql => {
            if let Adapter::MySql(adapter) = adapter {
                let source = MySqlDataSource::new(source, adapter).await?;
                Ok(Arc::new(source))
            } else {
                panic!("Invalid adapter type")
            }
        }
        DataFormat::Postgres => unimplemented!("Postgres data source not implemented"),
        _ => unimplemented!("Unsupported data source"),
    }
}
