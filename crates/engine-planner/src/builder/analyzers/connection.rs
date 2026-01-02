use crate::{
    builder::errors::ConnectionError,
    connection::test_connection,
    plan::connection::{
        plan::{ConnectionPlan, DatabaseDriver},
        status::{ConnectionRole, ConnectionStatus},
        utils::mask_url,
    },
};
use model::execution::connection::Connection;
use std::time::{Duration, Instant};
use tracing::info;

pub struct ConnectionAnalyzer {
    timeout: Duration,
}

impl ConnectionAnalyzer {
    pub fn new(timeout: Duration) -> Self {
        Self { timeout }
    }

    /// Analyze a connection by testing connectivity
    pub async fn analyze(
        &self,
        connection: &Connection,
    ) -> Result<ConnectionPlan, ConnectionError> {
        let start = Instant::now();
        let driver = Self::convert_driver(&connection.driver);
        let url = Self::get_url(connection);

        // Attempt connection with timeout
        let status = match tokio::time::timeout(
            self.timeout,
            test_connection(&connection.name, &url, &driver),
        )
        .await
        {
            Ok(result) => match result {
                Ok(test_result) => {
                    let latency_ms = start.elapsed().as_millis() as u64;
                    ConnectionStatus::Connected {
                        latency_ms,
                        version: test_result.version,
                    }
                }
                Err(e) => ConnectionStatus::Failed {
                    error: e.to_string(),
                },
            },
            Err(_) => ConnectionStatus::Failed {
                error: format!("Connection timeout after {:?}", self.timeout),
            },
        };

        info!(
            target: "analyzer",
            connection = %connection.name,
            status = ?status,
            "Connection analysis complete"
        );

        Ok(ConnectionPlan {
            name: connection.name.clone(),
            driver,
            url_masked: mask_url(&url),
            pool: None, // TODO: read from connection properties
            status,
            role: ConnectionRole::Both, // Will be refined based on usage
        })
    }

    fn convert_driver(driver: &str) -> DatabaseDriver {
        match driver.to_lowercase().as_str() {
            "postgres" | "postgresql" => DatabaseDriver::Postgres,
            "mysql" | "mariadb" => DatabaseDriver::MySql,
            "sqlite" => DatabaseDriver::Sqlite,
            "mssql" | "sqlserver" => DatabaseDriver::Mssql,
            "oracle" => DatabaseDriver::Oracle,
            other => DatabaseDriver::Other(other.to_string()),
        }
    }

    fn get_url(connection: &Connection) -> String {
        connection
            .properties
            .get("url")
            .map(|v| v.as_string().unwrap_or_default())
            .unwrap_or_default()
    }
}
