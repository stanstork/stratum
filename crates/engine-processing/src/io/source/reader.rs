use async_trait::async_trait;
use connectors::error::DriverError;
use model::pagination::{cursor::Cursor, page::FetchResult};

#[async_trait]
pub trait SourceReader: Send + Sync {
    async fn fetch(&self, batch_size: usize, cursor: Cursor) -> Result<FetchResult, DriverError>;
}
