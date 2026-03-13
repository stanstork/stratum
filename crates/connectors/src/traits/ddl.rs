use crate::{error::DriverError, sql::query::column::ColumnDef, traits::driver::Driver};
use async_trait::async_trait;

/// DDL operations for schema modifications.
#[async_trait]
pub trait DdlWriter: Driver {
    /// Add a column to an existing table.
    async fn add_column(&self, table: &str, column: &ColumnDef) -> Result<(), DriverError>;
}
