use crate::error::SinkError;
use async_trait::async_trait;

#[async_trait]
pub trait Sink<R>: Send + Sync
where
    R: Send + Sync + 'static,
{
    async fn write_batch(&self, batch: Vec<R>) -> Result<(), SinkError>;
}
