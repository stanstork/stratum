use super::providers::mysql::MySqlDataSource;
use crate::{adapter::Adapter, record::Record};
use async_trait::async_trait;
use smql::statements::connection::DataFormat;
use sql_adapter::adapter::SqlAdapter;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub enum DataSource {
    Database(Arc<Mutex<dyn DbDataSource>>),
}

impl DataSource {
    pub fn from_adapter(
        format: DataFormat,
        adapter: Adapter,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        match format {
            DataFormat::MySql => match adapter {
                Adapter::MySql(mysql_adapter) => {
                    let source = MySqlDataSource::new(mysql_adapter);
                    Ok(DataSource::Database(Arc::new(Mutex::new(source))))
                }
                _ => Err("Expected MySql adapter, but got a different type".into()),
            },
            DataFormat::Postgres => {
                // Add once implemented
                Err("Postgres data source is not implemented yet".into())
            }
            other => Err(format!("Unsupported data source format: {:?}", other).into()),
        }
    }
}

#[async_trait]
pub trait DbDataSource: Send + Sync {
    async fn fetch_data(
        &self,
        table: &str,
        batch_size: usize,
        offset: Option<usize>,
    ) -> Result<Vec<Record>, Box<dyn std::error::Error>>;
    fn adapter(&self) -> &(dyn SqlAdapter + Send + Sync);
}
