use crate::{
    error::DriverError,
    sql::metadata::{column::ColumnMetadata, table::TableMetadata},
    traits::driver::Driver,
};
use async_trait::async_trait;
use model::records::Record;

#[async_trait]
pub trait DataWriter: Driver {
    /// Write a batch of rows using standard INSERT statements.
    async fn write_batch(&self, meta: &TableMetadata, rows: &[Record]) -> Result<u64, DriverError>;

    /// Write rows using optimized bulk protocol (COPY/LOAD DATA).
    async fn copy_rows(
        &self,
        table: &str,
        columns: &[ColumnMetadata],
        rows: &[Record],
    ) -> Result<u64, DriverError>;

    /// Remove all rows from `table` (used by `replace` write mode).
    async fn truncate(&self, table: &str) -> Result<(), DriverError>;
}
