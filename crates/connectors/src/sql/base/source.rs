use crate::sql::base::{metadata::provider::MetadataHelper, requests::FetchRowsRequest};
use async_trait::async_trait;
use model::{pagination::cursor::Cursor, records::row::RowData};

#[async_trait]
pub trait DbDataSource: MetadataHelper + Send + Sync {
    type Error;

    async fn fetch(&self, batch_size: usize, cursor: Cursor) -> Result<Vec<RowData>, Self::Error>;

    fn build_fetch_rows_requests(&self, batch_size: usize, cursor: Cursor)
    -> Vec<FetchRowsRequest>;
}
