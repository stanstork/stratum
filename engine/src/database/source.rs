use super::connection::{DbConnection, MySqlConnection};
use super::mapping::TableMapping;
use super::metadata::TableMetadata;
use async_trait::async_trait;
use sqlx::Error;

#[async_trait]
pub trait DataSource {
    async fn fetch_data(&self, query: &str) -> Result<Vec<String>, sqlx::Error>;
}

pub struct MySqlDataSource {
    metadata: Vec<TableMetadata>,
    conn: MySqlConnection,
}

impl MySqlDataSource {
    pub async fn new(url: &str, mappings: Vec<TableMapping>) -> Result<Self, Error> {
        let conn = MySqlConnection::connect(url).await?;
        let mut metadata = Vec::new();

        for mapping in mappings {
            let table_metadata = TableMetadata::from_mapping(mapping, &conn).await?;
            metadata.push(table_metadata);
        }

        Ok(Self { metadata, conn })
    }

    pub fn metadata(&self) -> &Vec<TableMetadata> {
        &self.metadata
    }
}

#[async_trait]
impl DataSource for MySqlDataSource {
    async fn fetch_data(&self, query: &str) -> Result<Vec<String>, Error> {
        unimplemented!()
    }
}
