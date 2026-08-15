use crate::io::{
    format::DataFormat,
    sink::{Sink, mysql::MySqlSink, postgres::PostgresSink, wasm::WasmSinkAdapter},
};
use connectors::{
    drivers::{
        mysql::driver::MySqlDriver,
        postgres::{
            config::{CopyFormat, PgConflictAction},
            driver::PgDriver,
        },
    },
    error::DriverError,
    sql::metadata::table::TableMetadata,
    traits::driver::Driver,
};
use engine_core::schema::type_registry::Dialect;
use engine_wasm::runtime::instance::PluginInstance;
use model::{
    core::value::Value,
    execution::{
        connection::Connection,
        pipeline::DataDestination,
        tuning::{COPY_FORMAT, ON_CONFLICT},
    },
    records::Record,
};
use query_builder::{
    ast::load_data::LoadDataConflict,
    dialect::{self, QueryDialect},
};
use std::{collections::HashMap, sync::Arc};

/// Read a driver-specific tuning value from an endpoint's dialect block.
fn tuning<T>(
    tuning: &HashMap<String, Value>,
    key: &str,
    parse: fn(&str) -> Option<T>,
) -> Option<T> {
    match tuning.get(key) {
        Some(Value::String(s)) => parse(s),
        _ => None,
    }
}

/// Trait for creating a [`Destination`] from a typed driver.
pub trait IntoDestination: Driver {
    fn into_destination(
        self: Arc<Self>,
        dest: &DataDestination,
        source_dialect: Dialect,
    ) -> Destination;
}

impl IntoDestination for PgDriver {
    fn into_destination(
        self: Arc<Self>,
        dest: &DataDestination,
        source_dialect: Dialect,
    ) -> Destination {
        let driver = match tuning(&dest.tuning, COPY_FORMAT, CopyFormat::parse) {
            Some(format) => Arc::new((*self).clone().with_copy_format(format)),
            None => self,
        };
        let write_mode = dest.mode.clone();
        let on_conflict = tuning(&dest.tuning, ON_CONFLICT, PgConflictAction::parse);

        Destination {
            name: dest.table.clone(),
            format: DataFormat::Postgres,
            sink: Arc::new(PostgresSink::new(
                driver,
                source_dialect,
                write_mode,
                on_conflict,
            )),
        }
    }
}

impl IntoDestination for MySqlDriver {
    fn into_destination(
        self: Arc<Self>,
        dest: &DataDestination,
        _source_dialect: Dialect,
    ) -> Destination {
        let sink = MySqlSink::new(self);
        let sink = match tuning(&dest.tuning, ON_CONFLICT, LoadDataConflict::parse) {
            Some(on_conflict) => sink.with_on_conflict(on_conflict),
            None => sink,
        };

        Destination {
            name: dest.table.clone(),
            format: DataFormat::MySql,
            sink: Arc::new(sink),
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

    pub async fn truncate(&self, table: &str) -> Result<(), DriverError> {
        self.sink.truncate(table).await
    }

    pub async fn finalize(&self) -> Result<(), DriverError> {
        self.sink.finalize().await
    }

    pub fn query_dialect(&self) -> Option<Box<dyn QueryDialect>> {
        match self.format {
            DataFormat::Postgres => Some(Box::new(dialect::Postgres)),
            DataFormat::MySql => Some(Box::new(dialect::MySql)),
            _ => None,
        }
    }

    pub fn name(&self) -> String {
        self.name.clone()
    }

    pub fn sink(&self) -> Arc<dyn Sink + Send + Sync> {
        self.sink.clone()
    }
}
