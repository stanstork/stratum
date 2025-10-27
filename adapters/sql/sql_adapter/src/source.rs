use crate::{metadata::provider::MetadataHelper, requests::FetchRowsRequest};
use async_trait::async_trait;
use data_model::{pagination::cursor::Cursor, records::row_data::RowData};

#[async_trait]
pub trait DbDataSource: MetadataHelper + Send + Sync {
    type Error;

    async fn fetch(&self, batch_size: usize, cursor: Cursor) -> Result<Vec<RowData>, Self::Error>;

    fn build_fetch_rows_requests(&self, batch_size: usize, cursor: Cursor)
        -> Vec<FetchRowsRequest>;
}
