use crate::{
    context::env::EnvContext, drivers::DriverRef, plan::execution::ExecutionPlan,
    state::sled_store::SledStateStore,
};
use connectors::{
    drivers::{mysql::driver::MySqlDriver, postgres::driver::PgDriver},
    error::DriverError,
    traits::driver::Driver,
};
use model::execution::connection::Connection;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

/// Holds connections and file adapters for the duration of a migration.
#[derive(Clone)]
pub struct ExecutionContext {
    /// Connection pool - reuses drivers across pipelines
    connection_pool: Arc<RwLock<ConnectionPool>>,

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
