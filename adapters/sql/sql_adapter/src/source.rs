use crate::metadata::provider::MetadataHelper;
use async_trait::async_trait;
use common::row_data::RowData;

#[async_trait]
pub trait DbDataSource: MetadataHelper + Send + Sync {
    type Error;

    async fn fetch(
        &self,
        batch_size: usize,
        offset: Option<usize>,
    ) -> Result<Vec<RowData>, Self::Error>;
}
