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
        info!(url = %mask_url(&self.conn_str), "pinging MySQL");

        // connect
        let opts = mysql_async::Opts::from_url(&self.conn_str).map_err(|e| {
            error!(error = %e, "MySQL connection string parse failed");
            ConnectionError::Failed {
                name: self.name.clone(),
                reason: format!("Invalid connection string: {}", e),
            }
        })?;
        let pool = mysql_async::Pool::new(opts);
        let mut conn = pool.get_conn().await.map_err(|e| {
            error!(url = %mask_url(&self.conn_str), error = %e, "MySQL connection failed");
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
                error!(url = %mask_url(&self.conn_str), error = %e, "MySQL ping query failed");
                ConnectionError::Failed {
                    name: self.name.clone(),
                    reason: format!("Query failed: {}", e),
                }
            })?
            .ok_or_else(|| {
                error!(url = %mask_url(&self.conn_str), "MySQL ping returned no result");
                ConnectionError::Failed {
                    name: self.name.clone(),
                    reason: "Ping query returned no result".to_string(),
                }
            })?;

        // verify the result
        if val != 1 {
            error!(url = %mask_url(&self.conn_str), result = val, "MySQL ping returned unexpected result");
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
                error!(url = %mask_url(&self.conn_str), error = %e, "MySQL version query failed");
                ConnectionError::Failed {
                    name: self.name.clone(),
                    reason: format!("Version query failed: {}", e),
                }
            })?
            .unwrap_or_else(|| "unknown".to_string());

        info!(url = %mask_url(&self.conn_str), version = %version, "MySQL ping succeeded");
        drop(conn);
        pool.disconnect().await.ok();

        Ok(ConnectionTestResult { version })
    }
}

#[async_trait]
impl ConnectionTester for PostgresConnectionTester {
    async fn test(&self) -> Result<ConnectionTestResult, ConnectionError> {
        info!(url = %mask_url(&self.conn_str), "pinging Postgres");

        // connect
        let (client, connection) = tokio_postgres::connect(&self.conn_str, NoTls)
            .await
            .map_err(|e| {
                error!(url = %mask_url(&self.conn_str), error = %e, "Postgres connection failed");
                ConnectionError::Failed {
                    name: self.name.clone(),
                    reason: format!("Connection failed: {}", e),
                }
            })?;

        // spawn the connection handler
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!(error = %e, "Postgres connection error");
            }
        });

        // run the simple query
        let row = client.query_one("SELECT 1", &[]).await.map_err(|e| {
            error!(url = %mask_url(&self.conn_str), error = %e, "Postgres ping query failed");
            ConnectionError::Failed {
                name: self.name.clone(),
                reason: format!("Query failed: {}", e),
            }
        })?;

        // verify the result
        let val: i32 = row.get(0);
        if val != 1 {
            error!(url = %mask_url(&self.conn_str), result = val, "Postgres ping returned unexpected result");
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
                error!(url = %mask_url(&self.conn_str), error = %e, "Postgres version query failed");
                ConnectionError::Failed {
                    name: self.name.clone(),
                    reason: format!("Version query failed: {}", e),
                }
            })?;
        let version: String = version_row.get(0);

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
