use crate::{metadata::table::TableMetadata, row::RowData};
use async_trait::async_trait;

#[async_trait]
pub trait DbManager {
    async fn connect(url: &str) -> Result<Self, Box<dyn std::error::Error>>
    where
        Self: Sized;

    async fn table_exists(&self, table: &str) -> Result<bool, Box<dyn std::error::Error>>;
    async fn truncate_table(&self, table: &str) -> Result<(), Box<dyn std::error::Error>>;

    async fn fetch_metadata(
        &self,
        table: &str,
    ) -> Result<TableMetadata, Box<dyn std::error::Error>>;
    async fn fetch_all(&self, query: &str) -> Result<Vec<RowData>, Box<dyn std::error::Error>>;
}
