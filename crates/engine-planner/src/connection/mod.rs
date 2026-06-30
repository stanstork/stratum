use crate::{
    builder::errors::ConnectionError,
    plan::connection::{plan::DatabaseDriver, utils::mask_url},
};
use async_trait::async_trait;
use connectors::{
    drivers::{mysql::driver::MySqlDriver, postgres::driver::PgDriver},
    traits::driver::Driver,
};
use tracing::{error, info};

/// Result of a connection test
pub struct ConnectionTestResult {
    pub version: String,
}

#[async_trait]
pub trait ConnectionTester {
    async fn test(&self) -> Result<ConnectionTestResult, ConnectionError>;
}

/// MySQL/MariaDB connection tester
pub struct MySqlConnectionTester {
    pub name: String,
    pub conn_str: String,
}

/// Postgres connection tester
pub struct PostgresConnectionTester {
    pub name: String,
    pub conn_str: String,
}

#[async_trait]
impl ConnectionTester for MySqlConnectionTester {
    async fn test(&self) -> Result<ConnectionTestResult, ConnectionError> {
        info!(url = %mask_url(&self.conn_str), "pinging MySQL");

        let driver = MySqlDriver::connect(&self.conn_str).await.map_err(|e| {
            error!(url = %mask_url(&self.conn_str), error = %e, "MySQL connection failed");
            ConnectionError::Failed {
                name: self.name.clone(),
                reason: format!("Connection failed: {e}"),
            }
        })?;

        let version = driver.capabilities().version.clone();
        info!(url = %mask_url(&self.conn_str), version = %version, "MySQL ping succeeded");

        Ok(ConnectionTestResult { version })
    }
}

#[async_trait]
impl ConnectionTester for PostgresConnectionTester {
    async fn test(&self) -> Result<ConnectionTestResult, ConnectionError> {
        info!(url = %mask_url(&self.conn_str), "pinging Postgres");

        let driver = PgDriver::connect(&self.conn_str).await.map_err(|e| {
            error!(url = %mask_url(&self.conn_str), error = %e, "Postgres connection failed");
            ConnectionError::Failed {
                name: self.name.clone(),
                reason: format!("Connection failed: {e}"),
            }
        })?;

        let version = driver.capabilities().version.clone();
        info!(url = %mask_url(&self.conn_str), version = %version, "Postgres ping succeeded");

        Ok(ConnectionTestResult { version })
    }
}

pub async fn test_connection(
    name: &str,
    url: &str,
    driver: &DatabaseDriver,
) -> Result<ConnectionTestResult, ConnectionError> {
    match driver {
        DatabaseDriver::MySql => {
            MySqlConnectionTester {
                name: name.to_string(),
                conn_str: url.to_string(),
            }
            .test()
            .await
        }
        DatabaseDriver::Postgres => {
            PostgresConnectionTester {
                name: name.to_string(),
                conn_str: url.to_string(),
            }
            .test()
            .await
        }
        _ => Err(ConnectionError::Failed {
            name: name.to_string(),
            reason: format!("Unsupported database driver: {:?}", driver),
        }),
    }
}
