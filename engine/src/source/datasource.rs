use async_trait::async_trait;
use sql_adapter::row::RowData;

#[async_trait]
pub trait DataSource {
    async fn fetch_rows(&self) -> Result<Vec<RowData>, Box<dyn std::error::Error>>;
}
