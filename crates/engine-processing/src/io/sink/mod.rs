use crate::io::error::SinkError;
use async_trait::async_trait;
use connectors::{error::DriverError, sql::metadata::table::TableMetadata};
use model::records::Record;

pub mod mysql;
pub mod postgres;

#[async_trait]
pub trait Sink: Send + Sync {
    /// Write a batch of rows to the destination table.
    async fn write_batch(&self, meta: &TableMetadata, rows: &[Record]) -> Result<u64, DriverError>;

    /// Check if the sink supports the fast path (COPY + MERGE).
    async fn support_fast_path(&self) -> Result<bool, SinkError> {
        Ok(false)
    }

    /// Executes the fast-path write (COPY -> MERGE).
    async fn write_fast_path(
        &self,
        _table: &TableMetadata,
        _rows: &[Record],
    ) -> Result<(), SinkError> {
        Err(SinkError::FastPathNotSupported(
            "Fast path not implemented for this sink".to_string(),
        ))
    }
}
