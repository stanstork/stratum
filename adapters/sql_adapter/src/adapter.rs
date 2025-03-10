use crate::{metadata::table::TableMetadata, requests::FetchRowsRequest, row::row::RowData};
use async_trait::async_trait;

#[async_trait]
pub trait DbAdapter {
    async fn connect(url: &str) -> Result<Self, Box<dyn std::error::Error>>
    where
        Self: Sized;

    async fn table_exists(&self, table: &str) -> Result<bool, Box<dyn std::error::Error>>;
    async fn truncate_table(&self, table: &str) -> Result<(), Box<dyn std::error::Error>>;

    async fn execute(&self, query: &str) -> Result<(), Box<dyn std::error::Error>>;
    async fn fetch_metadata(
        &self,
        table: &str,
    ) -> Result<TableMetadata, Box<dyn std::error::Error>>;
    async fn fetch_rows(
        &self,
        request: FetchRowsRequest,
    ) -> Result<Vec<RowData>, Box<dyn std::error::Error>>;
}
