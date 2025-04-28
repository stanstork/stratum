use crate::{metadata::provider::MetadataHelper, row::row_data::RowData};
use async_trait::async_trait;

#[async_trait]
pub trait DbDataSource: MetadataHelper + Send + Sync {
    async fn fetch(
        &self,
        batch_size: usize,
        offset: Option<usize>,
    ) -> Result<Vec<RowData>, Box<dyn std::error::Error>>;
}
