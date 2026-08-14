use crate::{context::exec::ConnectionPool, dispatch_driver, schema::type_registry::Dialect};
use connectors::{
    drivers::{mysql::driver::MySqlDriver, postgres::driver::PgDriver},
    error::DriverError,
    sql::metadata::table::TableMetadata,
    traits::introspector::SchemaIntrospector,
};
use model::execution::connection::Connection;
use std::sync::Arc;

pub mod macros;

/// Unified handle for any supported database driver.
#[derive(Clone)]
pub enum DriverRef {
    Postgres(Arc<PgDriver>),
    MySql(Arc<MySqlDriver>),
}

impl DriverRef {
    pub fn dialect(&self) -> Dialect {
        match self {
            Self::Postgres(_) => Dialect::Postgres,
            Self::MySql(_) => Dialect::MySql,
        }
    }

    /// Resolve a single driver from the connection pool.
    pub async fn resolve(
        driver_str: &str,
        connection: &Connection,
        connections: &mut ConnectionPool,
    ) -> Result<Self, DriverError> {
        match driver_str {
            "postgres" | "postgresql" => {
                let d = connections.postgres(connection).await?;
                Ok(DriverRef::Postgres(d))
            }
            "mysql" => {
                let d = connections.mysql(connection).await?;
                Ok(DriverRef::MySql(d))
            }
            other => Err(DriverError::UnsupportedDriver(format!(
                "Driver '{}' not supported",
                other
            ))),
        }
    }

    /// A driver handle backed by its own connection, so a parallel lane writes
    /// independently of the others. Postgres is a single connection, so this
    /// opens a fresh one (same URL/schema); MySQL is already pool-backed and its
    /// pool hands out concurrent connections, so it is reused as-is.
    pub async fn reconnect(&self) -> Result<DriverRef, DriverError> {
        match self {
            Self::Postgres(d) => {
                let fresh = PgDriver::connect_with_schema(d.url(), d.schema()).await?;
                Ok(DriverRef::Postgres(Arc::new(fresh)))
            }
            Self::MySql(_) => Ok(self.clone()),
        }
    }

    pub async fn table_metadata(&self, table: &str) -> Result<TableMetadata, DriverError> {
        dispatch_driver!(self, |d| Ok(d.table_metadata(table).await?))
    }

    /// Extract PostgreSQL driver if this is a Postgres variant.
    pub fn as_postgres(&self) -> Option<&Arc<PgDriver>> {
        match self {
            Self::Postgres(d) => Some(d),
            _ => None,
        }
    }

    /// Extract MySQL driver if this is a MySQL variant.
    pub fn as_mysql(&self) -> Option<&Arc<MySqlDriver>> {
        match self {
            Self::MySql(d) => Some(d),
            _ => None,
        }
    }
}
