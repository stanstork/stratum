use crate::{metadata::provider::MetadataHelper, requests::FetchRowsRequest};
use async_trait::async_trait;
use chrono::Duration;
use common::row_data::RowData;
use query_builder::offsets::{Cursor, OffsetStrategy};
use tokio_util::sync::CancellationToken;

#[async_trait]
pub trait DbDataSource: MetadataHelper + Send + Sync {
    type Error;

    async fn fetch(
        &self,
        batch_size: usize,
        max_ms: Duration,
        cancel: &CancellationToken,
        cursor: Cursor,
        start: &dyn OffsetStrategy,
    ) -> Result<Vec<RowData>, Self::Error>;

    fn build_fetch_rows_requests(
        &self,
        batch_size: usize,
        cursor: Cursor,
        start: &dyn OffsetStrategy,
    ) -> Vec<FetchRowsRequest>;
}
