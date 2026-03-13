use crate::{
    drivers::mysql::driver::MySqlDriver,
    error::DriverError,
    sql::query::{column::ColumnDef, generator::QueryGenerator},
    traits::{ddl::DdlWriter, executor::QueryExecutor},
};
use async_trait::async_trait;
use query_builder::dialect;
use tracing::info;

#[async_trait]
impl DdlWriter for MySqlDriver {
    async fn add_column(&self, table: &str, column: &ColumnDef) -> Result<(), DriverError> {
        let (sql, _params) = QueryGenerator::new(&dialect::MySql).add_column(table, column.clone());

        info!("Adding column {} to table {}", column.name, table);

        self.execute(&sql).await?;

        Ok(())
    }
}
