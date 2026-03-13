use crate::io::{
    format::DataFormat,
    sink::{Sink, postgres::PostgresSink},
};
use connectors::{
    drivers::postgres::driver::PgDriver, error::DriverError, sql::metadata::table::TableMetadata,
};
use engine_core::schema::type_registry::Dialect;
use model::{execution::connection::Connection, records::Record};
use query_builder::dialect;
use std::sync::Arc;

#[derive(Clone)]
pub struct Destination {
    pub name: String,
    pub format: DataFormat,
    pub sink: Arc<dyn Sink + Send + Sync>,
}

impl Destination {
    /// Create a new PostgreSQL destination.
    pub fn postgres(driver: Arc<PgDriver>, table: &str, source_dialect: Dialect) -> Self {
        Destination {
            name: table.to_string(),
            format: DataFormat::Postgres,
            sink: Arc::new(PostgresSink::new(driver, source_dialect)),
        }
    }

    /// Create a destination from connection info.
    /// Note: For PostgreSQL, prefer using `postgres()` with a typed driver.
    pub fn new(
        sink: Arc<dyn Sink + Send + Sync>,
        table: &str,
        conn: &Connection,
    ) -> Result<Self, DriverError> {
        let name = table.to_string();
        let format = DataFormat::parse(&conn.driver)
            .ok_or_else(|| DriverError::UnsupportedFormat(conn.driver.clone()))?;

        Ok(Destination { name, format, sink })
    }

    pub async fn write_batch(
        &self,
        meta: &TableMetadata,
        rows: &[Record],
    ) -> Result<u64, DriverError> {
        self.sink.write_batch(meta, rows).await
    }

    pub fn dialect(&self) -> Box<dyn dialect::Dialect> {
        match self.format {
            DataFormat::Postgres => Box::new(dialect::Postgres),
            DataFormat::MySql => Box::new(dialect::MySql),
            _ => panic!("Unsupported format: {:?}", self.format),
        }
    }

    pub fn name(&self) -> String {
        self.name.clone()
    }

    pub fn sink(&self) -> Arc<dyn Sink + Send + Sync> {
        self.sink.clone()
    }
}
