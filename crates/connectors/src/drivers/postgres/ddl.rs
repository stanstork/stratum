use crate::{
    drivers::postgres::driver::PgDriver,
    error::DriverError,
    sql::query::{column::ColumnDef, generator::QueryGenerator},
    traits::{ddl::DdlWriter, executor::QueryExecutor},
};
use async_trait::async_trait;
use query_builder::dialect;
use tracing::debug;

#[async_trait]
impl DdlWriter for PgDriver {
    async fn add_column(&self, table: &str, column: &ColumnDef) -> Result<(), DriverError> {
        let (sql, _params) =
            QueryGenerator::new(&dialect::Postgres).add_column(table, column.clone());

        debug!("Adding column {} to table {}", column.name, table);

        self.execute(&sql).await?;

        Ok(())
    }
}
