use crate::error::SinkError;
use async_trait::async_trait;
use connectors::sql::base::metadata::table::TableMetadata;
use model::records::batch::Batch;

pub mod postgres;

#[async_trait]
pub trait Sink: Send + Sync {
    async fn support_fast_path(&self) -> Result<bool, SinkError>;

    /// Executes the fast-path write (COPY -> MERGE).
    async fn write_fast_path(&self, table: &TableMetadata, batch: &Batch) -> Result<(), SinkError>;
}
