use super::{providers::mysql::MySqlDataSource, record::DataRecord};
use crate::config::config::Config;
use async_trait::async_trait;
use sql_adapter::db_type::DbType;

pub enum DataSourceType {
    File,
    Database(DbType),
    Api,
    // Add more types as needed
}

#[async_trait]
pub trait DataSource {
    type Record: DataRecord + Send + Sync + 'static;

    async fn fetch_data(&self) -> Result<Vec<Self::Record>, Box<dyn std::error::Error>>;
}

pub async fn create_data_source(
    source_type: DataSourceType,
    config: &Config,
) -> Result<Box<dyn DataSource<Record = Box<dyn DataRecord>>>, Box<dyn std::error::Error>> {
    match source_type {
        DataSourceType::File => {
            // Implement file data source creation
            unimplemented!("File data source not implemented")
        }
        DataSourceType::Database(db_type) => match db_type {
            DbType::MySql => {
                let source =
                    MySqlDataSource::new(&config.source, config.mappings[0].clone()).await?;
                Ok(Box::new(source))
            }
            DbType::Postgres => unimplemented!("Postgres data source not implemented"),
            DbType::Other(_) => unimplemented!("Other database types not implemented"),
        },
        DataSourceType::Api => {
            // Implement API data source creation
            unimplemented!("API data source not implemented")
        }
    }
}
