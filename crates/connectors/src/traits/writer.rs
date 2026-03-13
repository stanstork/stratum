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
        _table: &str,
        _columns: &[ColumnMetadata],
        _rows: &[Record],
    ) -> Result<u64, DriverError> {
        unimplemented!("copy_rows not implemented for this driver");
    }
}
