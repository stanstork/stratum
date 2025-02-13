use crate::database::row::RowData;
use async_trait::async_trait;

#[async_trait]
pub trait DataSource {
    async fn fetch_data(&self) -> Result<Vec<RowData>, sqlx::Error>;
}
