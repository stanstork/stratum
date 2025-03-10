use super::{providers::mysql::MySqlDataSource, record::DataRecord};
use async_trait::async_trait;
use smql::statements::connection::DataFormat;
use sql_adapter::adapter::DbAdapter;

#[async_trait]
pub trait DataSource: Send + Sync {
    type Record: DataRecord + Send + Sync + 'static;

    async fn fetch_data(
        &self,
        batch_size: usize,
        offset: Option<usize>,
    ) -> Result<Vec<Self::Record>, Box<dyn std::error::Error>>;
}

pub async fn create_data_source(
    source: String,
    data_format: DataFormat,
    adapter: Box<dyn DbAdapter + Send + Sync>,
) -> Result<
    Box<dyn DataSource<Record = Box<dyn DataRecord + Send + Sync>>>,
    Box<dyn std::error::Error>,
> {
    match data_format {
        DataFormat::MySql => {
            let source = MySqlDataSource::new(&source, adapter).await?;
            Ok(Box::new(source))
        }
        DataFormat::Postgres => unimplemented!("Postgres data source not implemented"),
        _ => unimplemented!("Unsupported data source"),
    }
}
