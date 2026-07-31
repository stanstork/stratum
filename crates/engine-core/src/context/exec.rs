use crate::{
    context::env::EnvContext,
    drivers::DriverRef,
    plan::execution::ExecutionPlan,
    schema::{metadata_cache::MetadataCache, type_registry::Dialect},
    state::sled_store::SledStateStore,
};
use connectors::{
    drivers::{mysql::driver::MySqlDriver, postgres::driver::PgDriver},
    error::DriverError,
    traits::{driver::Driver, introspector::SchemaIntrospector},
};
use model::execution::connection::Connection;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::RwLock;

const METADATA_CACHE_TIMEOUT: Duration = Duration::from_secs(30);

/// Holds connections and file adapters for the duration of a migration.
#[derive(Clone)]
pub struct ExecutionContext {
    /// Connection pool - reuses drivers across pipelines
    connection_pool: Arc<RwLock<ConnectionPool>>,

    /// Read-through metadata caches, shared across every pipeline.
    /// Only source (read-only) introspection is cached here; the
    /// destination is mutated by DDL mid-pipeline and must not be cached.
    metadata_caches: Arc<RwLock<HashMap<String, Arc<dyn SchemaIntrospector>>>>,

    pub run_id: String,
    pub state: Arc<SledStateStore>,
    pub env: Arc<EnvContext>,
}

impl ExecutionContext {
    pub async fn new(
        plan: &ExecutionPlan,
        state: Arc<SledStateStore>,
        env: Arc<EnvContext>,
    ) -> Result<Self, DriverError> {
        let run_id = plan.run_id();

        Ok(ExecutionContext {
            connection_pool: Arc::new(RwLock::new(ConnectionPool::new())),
            metadata_caches: Arc::new(RwLock::new(HashMap::new())),
            run_id,
            state,
            env,
        })
    }

    pub fn run_id(&self) -> String {
        self.run_id.clone()
    }

    /// Get a driver from the pool (trait object).
    pub async fn get_driver(&self, conn: &Connection) -> Result<Arc<dyn Driver>, DriverError> {
        let mut pool = self.connection_pool.write().await;
        pool.get_or_create(conn).await
    }

    /// Get a typed PostgreSQL driver for full capability access.
    pub async fn get_pg_driver(&self, conn: &Connection) -> Result<Arc<PgDriver>, DriverError> {
        let mut pool = self.connection_pool.write().await;
        pool.get_or_create_postgres(conn).await
    }

    /// Get a typed MySQL driver for full capability access.
    pub async fn get_mysql_driver(
        &self,
        conn: &Connection,
    ) -> Result<Arc<MySqlDriver>, DriverError> {
        let mut pool = self.connection_pool.write().await;
        pool.get_or_create_mysql(conn).await
    }

    /// Resolve a connection to a typed `DriverRef`, reusing pooled connections.
    pub async fn resolve_driver(&self, conn: &Connection) -> Result<DriverRef, DriverError> {
        let mut pool = self.connection_pool.write().await;
        DriverRef::resolve(&conn.driver, conn, &mut pool).await
    }

    /// A read-through introspector for a **source** connection,
    /// cached by `conn.name` and shared across pipelines.
    pub async fn cached_source_introspector(
        &self,
        conn: &Connection,
    ) -> Result<Arc<dyn SchemaIntrospector>, DriverError> {
        if let Some(cache) = self.metadata_caches.read().await.get(&conn.name) {
            return Ok(cache.clone());
        }

        let dialect = Dialect::parse(&conn.driver)
            .ok_or_else(|| DriverError::UnsupportedScheme(conn.driver.clone()))?;

        let cache: Arc<dyn SchemaIntrospector> = match conn.driver.as_str() {
            "postgres" | "postgresql" => Arc::new(MetadataCache::new(
                self.get_pg_driver(conn).await?,
                dialect,
                METADATA_CACHE_TIMEOUT,
            )),
            "mysql" => Arc::new(MetadataCache::new(
                self.get_mysql_driver(conn).await?,
                dialect,
                METADATA_CACHE_TIMEOUT,
            )),
            other => return Err(DriverError::UnsupportedScheme(other.to_string())),
        };

        self.metadata_caches
            .write()
            .await
            .insert(conn.name.clone(), cache.clone());
        Ok(cache)
    }
}

/// Connection pool for reusing drivers.
pub struct ConnectionPool {
    pg_drivers: HashMap<String, Arc<PgDriver>>,
    mysql_drivers: HashMap<String, Arc<MySqlDriver>>,
}

impl ConnectionPool {
    pub fn new() -> Self {
        ConnectionPool {
            pg_drivers: HashMap::new(),
            mysql_drivers: HashMap::new(),
        }
    }

    pub async fn get_or_create(
        &mut self,
        conn: &Connection,
    ) -> Result<Arc<dyn Driver>, DriverError> {
        match conn.driver.as_str() {
            "postgres" | "postgresql" => {
                let driver = self.get_or_create_postgres(conn).await?;
                Ok(driver as Arc<dyn Driver>)
            }
            "mysql" => {
                let driver = self.get_or_create_mysql(conn).await?;
                Ok(driver as Arc<dyn Driver>)
            }
            driver => Err(DriverError::UnsupportedScheme(driver.to_string())),
        }
    }

    /// Get or create a PostgreSQL driver with full type information.
    pub async fn get_or_create_postgres(
        &mut self,
        conn: &Connection,
    ) -> Result<Arc<PgDriver>, DriverError> {
        if let Some(driver) = self.pg_drivers.get(&conn.name) {
            return Ok(driver.clone());
        }

        let url = conn
            .properties
            .get_string("url")
            .ok_or_else(|| DriverError::InvalidUrl("missing 'url' property".to_string()))?;

        let driver = match conn.properties.get_string("schema") {
            Some(schema) => PgDriver::connect_with_schema(&url, &schema).await?,
            None => PgDriver::connect(&url).await?,
        };
        let driver = Arc::new(driver);
        self.pg_drivers.insert(conn.name.clone(), driver.clone());
        Ok(driver)
    }

    /// Get or create a MySQL driver with full type information.
    pub async fn get_or_create_mysql(
        &mut self,
        conn: &Connection,
    ) -> Result<Arc<MySqlDriver>, DriverError> {
        if let Some(driver) = self.mysql_drivers.get(&conn.name) {
            return Ok(driver.clone());
        }

        let url = conn
            .properties
            .get_string("url")
            .ok_or_else(|| DriverError::InvalidUrl("missing 'url' property".to_string()))?;

        let driver = Arc::new(MySqlDriver::connect(&url).await?);
        self.mysql_drivers.insert(conn.name.clone(), driver.clone());
        Ok(driver)
    }
}

impl Default for ConnectionPool {
    fn default() -> Self {
        Self::new()
    }
}
