use crate::io::{
    format::DataFormat,
    sink::{Sink, mysql::MySqlSink, postgres::PostgresSink, wasm::WasmSinkAdapter},
};
use connectors::{
    drivers::{mysql::driver::MySqlDriver, postgres::driver::PgDriver},
    error::DriverError,
    sql::metadata::table::TableMetadata,
    traits::driver::Driver,
};
use engine_core::schema::type_registry::Dialect;
use engine_wasm::runtime::instance::PluginInstance;
use model::{execution::connection::Connection, records::Record};
use query_builder::dialect;
use std::sync::Arc;

/// Trait for creating a [`Destination`] from a typed driver.
pub trait IntoDestination: Driver {
    fn into_destination(self: Arc<Self>, table: &str, source_dialect: Dialect) -> Destination;
}

impl IntoDestination for PgDriver {
    fn into_destination(self: Arc<Self>, table: &str, source_dialect: Dialect) -> Destination {
        Destination {
            name: table.to_string(),
            format: DataFormat::Postgres,
            sink: Arc::new(PostgresSink::new(self, source_dialect)),
        }
    }
}

impl IntoDestination for MySqlDriver {
    fn into_destination(self: Arc<Self>, table: &str, source_dialect: Dialect) -> Destination {
        Destination {
            name: table.to_string(),
            format: DataFormat::MySql,
            sink: Arc::new(MySqlSink::new(self, source_dialect)),
        }
    }
}

#[derive(Clone)]
pub struct Destination {
    pub name: String,
    pub format: DataFormat,
    pub sink: Arc<dyn Sink + Send + Sync>,
}

impl Destination {
    /// Create a destination from connection info and a pre-built sink.
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

    pub fn wasm(plugin: PluginInstance, table: &str) -> Self {
        let name = table.to_string();
        let format = DataFormat::Wasm;
        let sink = Arc::new(WasmSinkAdapter::new(plugin));
        Destination { name, format, sink }
    }

    pub async fn write_batch(
        &self,
        meta: &TableMetadata,
        rows: &[Record],
    ) -> Result<u64, DriverError> {
        self.sink.write_batch(meta, rows).await
    }

    pub async fn prepare(&self) -> Result<(), DriverError> {
        self.sink.prepare().await
    }

    pub async fn finalize(&self) -> Result<(), DriverError> {
        self.sink.finalize().await
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
