use crate::{
    error::DriverError,
    sql::{metadata::table::TableMetadata, query::column::ColumnDef},
    traits::executor::QueryExecutor,
};
use async_trait::async_trait;

/// DDL operations for schema modifications.
#[async_trait]
pub trait DdlWriter: QueryExecutor {
    /// Add a column to an existing table.
    async fn add_column(&self, table: &str, column: &ColumnDef) -> Result<(), DriverError>;

    /// Drop each table's primary key before a bulk load, returning the DDL to
    /// rebuild them afterwards.
    ///
    /// The default is a no-op that keeps every primary key in place - override
    /// it only for engines whose indexes build faster in bulk after the load.
    async fn drop_primary_keys(
        &self,
        _metas: &[TableMetadata],
    ) -> Result<Vec<String>, DriverError> {
        Ok(Vec::new())
    }

    /// Execute a sequence of DDL statements against this driver, in order.
    async fn execute_ddl(&self, statements: &[String]) -> Result<(), DriverError> {
        for sql in statements {
            self.execute(sql).await?;
        }
        Ok(())
    }
}
