use crate::{plan::execution::ExecutionPlan, state::sled_store::SledStateStore};
use connectors::{
    adapter::Adapter, driver::SqlDriver, error::AdapterError, file::csv::settings::CsvSettings,
};
use model::execution::connection::Connection;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

/// Holds connections and file adapters for the duration of a migration.
#[derive(Clone)]
pub struct ExecutionContext {
    /// Connection pool - reuses adapters across pipelines
    connection_pool: Arc<RwLock<ConnectionPool>>,

    pub run_id: String,
    pub state: Arc<SledStateStore>,
}

impl ExecutionContext {
    pub async fn new(
        plan: &ExecutionPlan,
        state: Arc<SledStateStore>,
    ) -> Result<Self, AdapterError> {
        // Generate a deterministic run_id based on the plan hash
        // This allows resuming the same migration after restart
        let run_id = format!("run-{}", &plan.hash()[..16]);

        Ok(ExecutionContext {
            connection_pool: Arc::new(RwLock::new(ConnectionPool::new())),
            run_id,
            state,
        })
    }

    pub fn run_id(&self) -> String {
        self.run_id.clone()
    }

    pub async fn get_adapter(&self, conn: &Connection) -> Result<Adapter, AdapterError> {
        let mut pool = self.connection_pool.write().await;
        pool.get_or_create(conn).await
    }
}

/// Connection pool for reusing adapters
struct ConnectionPool {
    adapters: HashMap<String, Adapter>,
}

impl ConnectionPool {
    fn new() -> Self {
        ConnectionPool {
            adapters: HashMap::new(),
        }
    }

    async fn get_or_create(&mut self, conn: &Connection) -> Result<Adapter, AdapterError> {
        if let Some(adapter) = self.adapters.get(&conn.name) {
            return Ok(adapter.clone());
        }

        let adapter = match conn.driver.as_str() {
            "postgres" | "postgresql" => {
                let url = conn
                    .properties
                    .get_string("url")
                    .ok_or_else(|| AdapterError::MissingProperty("url".to_string()))?;

                Adapter::sql(SqlDriver::Postgres, &url).await?
            }
            "mysql" => {
                let url = conn
                    .properties
                    .get_string("url")
                    .ok_or_else(|| AdapterError::MissingProperty("url".to_string()))?;

                Adapter::sql(SqlDriver::MySql, &url).await?
            }
            "csv" => {
                let path = conn
                    .properties
                    .get_string("path")
                    .ok_or_else(|| AdapterError::MissingProperty("path".to_string()))?;

                let settings = CsvSettings {
                    delimiter: conn
                        .properties
                        .get_string("delimiter")
                        .and_then(|v| v.chars().next())
                        .unwrap_or(','),
                    has_headers: conn.properties.get_bool("has_header").unwrap_or(true),
                    pk_column: conn.properties.get_string("id_column"),
                    sample_size: conn.properties.get_usize("sample_size").unwrap_or(50),
                };

                Adapter::file(&path, settings)?
            }
            driver => {
                return Err(AdapterError::UnsupportedDriver(driver.to_string()));
            }
        };

        // Cache the adapter
        self.adapters.insert(conn.name.clone(), adapter.clone());
        Ok(adapter)
    }
}
