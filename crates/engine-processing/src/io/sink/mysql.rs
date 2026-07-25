use crate::io::{error::SinkError, sink::Sink};
use async_trait::async_trait;
use connectors::{
    drivers::mysql::driver::MySqlDriver,
    error::DriverError,
    sql::metadata::{column::ColumnMetadata, table::TableMetadata},
    traits::{driver::Driver, writer::DataWriter},
};
use model::records::Record;
use query_builder::ast::load_data::LoadDataConflict;
use std::sync::Arc;
use tracing::debug;

pub struct MySqlSink {
    driver: Arc<MySqlDriver>,
    on_conflict: LoadDataConflict,
}

impl MySqlSink {
    pub fn new(driver: Arc<MySqlDriver>) -> Self {
        Self {
            driver,
            on_conflict: LoadDataConflict::Default,
        }
    }

    pub fn with_on_conflict(mut self, on_conflict: LoadDataConflict) -> Self {
        self.on_conflict = on_conflict;
        self
    }

    fn columns(&self, table: &TableMetadata) -> Vec<ColumnMetadata> {
        table.columns.values().cloned().collect()
    }
}

#[async_trait]
impl Sink for MySqlSink {
    async fn write_batch(&self, meta: &TableMetadata, rows: &[Record]) -> Result<u64, DriverError> {
        self.driver.write_batch(meta, rows).await
    }

    async fn truncate(&self, table: &str) -> Result<(), DriverError> {
        self.driver.truncate(table).await
    }

    async fn support_fast_path(&self) -> Result<bool, SinkError> {
        let capabilities = self.driver.capabilities();
        // Fast path requires LOAD DATA INFILE support
        Ok(capabilities.copy_protocol)
    }

    async fn write_fast_path(
        &self,
        table: &TableMetadata,
        rows: &[Record],
    ) -> Result<(), SinkError> {
        if rows.is_empty() {
            return Ok(());
        }

        let columns = self.columns(table);

        debug!(table = %table.name, rows = rows.len(), on_conflict = ?self.on_conflict, "fast-path LOAD DATA");

        self.driver
            .load_data(&table.name, &columns, rows, self.on_conflict)
            .await?;
        Ok(())
    }
}
