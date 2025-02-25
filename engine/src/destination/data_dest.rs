use crate::source::record::DataRecord;
use async_trait::async_trait;

#[async_trait]
pub trait DataDestination {
    type Record: DataRecord + Send + Sync + 'static;

    async fn write(&self, data: Vec<Self::Record>) -> Result<(), Box<dyn std::error::Error>>;
}
