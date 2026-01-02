use crate::{
    builder::errors::ConnectionError,
    plan::connection::{plan::DatabaseDriver, utils::mask_url},
};
use async_trait::async_trait;
use mysql_async::prelude::Queryable;
use tokio_postgres::NoTls;
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
        info!("Pinging MySQL at '{}'", mask_url(&self.conn_str));

        // connect
        let opts = mysql_async::Opts::from_url(&self.conn_str).map_err(|e| {
            error!("MySQL connection string parse failed: {}", e);
            ConnectionError::Failed {
                name: self.name.clone(),
                reason: format!("Invalid connection string: {}", e),
            }
        })?;
        let pool = mysql_async::Pool::new(opts);
        let mut conn = pool.get_conn().await.map_err(|e| {
            error!(
                "MySQL connection to '{}' failed: {}",
                mask_url(&self.conn_str),
                e
            );
            ConnectionError::Failed {
                name: self.name.clone(),
                reason: format!("Connection failed: {}", e),
            }
        })?;

        // run the simple query
        let val: i32 = conn
            .query_first("SELECT 1")
            .await
            .map_err(|e| {
                error!(
                    "MySQL ping query on '{}' failed: {}",
                    mask_url(&self.conn_str),
                    e
                );
                ConnectionError::Failed {
                    name: self.name.clone(),
                    reason: format!("Query failed: {}", e),
                }
            })?
            .ok_or_else(|| {
                let msg = format!(
                    "MySQL ping to '{}' returned no result",
                    mask_url(&self.conn_str)
                );
                error!("{}", msg);
                ConnectionError::Failed {
                    name: self.name.clone(),
                    reason: "Ping query returned no result".to_string(),
                }
            })?;

        // verify the result
        if val != 1 {
            let msg = format!(
                "MySQL ping to '{}' returned unexpected result: {}",
                mask_url(&self.conn_str),
                val
            );
            error!("{}", msg);
            return Err(ConnectionError::Failed {
                name: self.name.clone(),
                reason: format!("Unexpected result: {}", val),
            });
        }

        // get version
        let version: String = conn
            .query_first("SELECT VERSION()")
            .await
            .map_err(|e| {
                error!(
                    "MySQL version query on '{}' failed: {}",
                    mask_url(&self.conn_str),
                    e
                );
                ConnectionError::Failed {
                    name: self.name.clone(),
                    reason: format!("Version query failed: {}", e),
                }
            })?
            .unwrap_or_else(|| "unknown".to_string());

        info!(
            "MySQL ping to '{}' succeeded, version: {}",
            mask_url(&self.conn_str),
            version
        );
        drop(conn);
        pool.disconnect().await.ok();

        Ok(ConnectionTestResult { version })
    }
}

#[async_trait]
impl ConnectionTester for PostgresConnectionTester {
    async fn test(&self) -> Result<ConnectionTestResult, ConnectionError> {
        info!("Pinging Postgres at '{}'", mask_url(&self.conn_str));

        // connect
        let (client, connection) = tokio_postgres::connect(&self.conn_str, NoTls)
            .await
            .map_err(|e| {
                error!(
                    "Postgres connection to '{}' failed: {}",
                    mask_url(&self.conn_str),
                    e
                );
                ConnectionError::Failed {
                    name: self.name.clone(),
                    reason: format!("Connection failed: {}", e),
                }
            })?;

        // spawn the connection handler
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("Postgres connection error: {}", e);
            }
        });

        // run the simple query
        let row = client.query_one("SELECT 1", &[]).await.map_err(|e| {
            error!(
                "Postgres ping query on '{}' failed: {}",
                mask_url(&self.conn_str),
                e
            );
            ConnectionError::Failed {
                name: self.name.clone(),
                reason: format!("Query failed: {}", e),
            }
        })?;

        // verify the result
        let val: i32 = row.get(0);
        if val != 1 {
            let msg = format!(
                "Postgres ping to '{}' returned unexpected result: {}",
                mask_url(&self.conn_str),
                val
            );
            error!("{}", msg);
            return Err(ConnectionError::Failed {
                name: self.name.clone(),
                reason: format!("Unexpected result: {}", val),
            });
        }

        // get version
        let version_row = client
            .query_one("SELECT version()", &[])
            .await
            .map_err(|e| {
                error!(
                    "Postgres version query on '{}' failed: {}",
                    mask_url(&self.conn_str),
                    e
                );
                ConnectionError::Failed {
                    name: self.name.clone(),
                    reason: format!("Version query failed: {}", e),
                }
            })?;
        let version: String = version_row.get(0);

        info!(
            "Postgres ping to '{}' succeeded, version: {}",
            mask_url(&self.conn_str),
            version
        );
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
