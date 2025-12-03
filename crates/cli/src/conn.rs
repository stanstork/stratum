use crate::error::CliError;
use async_trait::async_trait;
use mysql_async::prelude::*;
use smql_syntax::ast::connection::DataFormat;
use std::str::FromStr;
use tokio_postgres::NoTls;
use tracing::{error, info};

/// What kind of connection to check
#[derive(Debug)]
pub enum ConnectionKind {
    MySql,
    Postgres,
    Ftp,
}

impl FromStr for ConnectionKind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "mysql" | "mariadb" => Ok(ConnectionKind::MySql),
            "pg" | "postgres" | "postgresql" => Ok(ConnectionKind::Postgres),
            "ftp" => Ok(ConnectionKind::Ftp),
            other => Err(format!("Unknown connection kind: {other}")),
        }
    }
}

impl ConnectionKind {
    pub fn data_format(&self) -> DataFormat {
        match self {
            ConnectionKind::MySql => DataFormat::MySql,
            ConnectionKind::Postgres => DataFormat::Postgres,
            ConnectionKind::Ftp => DataFormat::Csv,
        }
    }
}

/// Trait for "pinging" a data source
#[async_trait]
pub trait ConnectionPinger {
    /// Attempts to ping; returns Err if unreachable
    async fn ping(&self) -> Result<(), CliError>;
}

/// MySQL/MariaDB pinger
pub struct MySqlConnectionPinger {
    pub conn_str: String,
}

/// Postgres pinger
pub struct PostgresConnectionPinger {
    pub conn_str: String,
}

#[async_trait]
impl ConnectionPinger for MySqlConnectionPinger {
    async fn ping(&self) -> Result<(), CliError> {
        info!("Pinging MySQL at '{}'", &self.conn_str);

        // connect
        let opts = mysql_async::Opts::from_url(&self.conn_str).map_err(|e| {
            error!("MySQL connection string parse failed: {}", e);
            CliError::MySql(mysql_async::Error::Url(e))
        })?;
        let pool = mysql_async::Pool::new(opts);
        let mut conn = pool.get_conn().await.map_err(|e| {
            error!("MySQL connection to '{}' failed: {}", &self.conn_str, e);
            CliError::MySql(e)
        })?;

        // run the simple query
        let val: i32 = conn
            .query_first("SELECT 1")
            .await
            .map_err(|e| {
                error!("MySQL ping query on '{}' failed: {}", &self.conn_str, e);
                CliError::MySql(e)
            })?
            .ok_or_else(|| {
                let msg = format!("MySQL ping to '{}' returned no result", &self.conn_str);
                error!("{}", msg);
                CliError::Unexpected(msg)
            })?;

        // verify the result
        if val != 1 {
            let msg = format!(
                "MySQL ping to '{}' returned unexpected result: {}",
                &self.conn_str, val
            );
            error!("{}", msg);
            return Err(CliError::Unexpected(msg));
        }

        info!("MySQL ping to '{}' succeeded", &self.conn_str);
        drop(conn);
        pool.disconnect().await.ok();
        Ok(())
    }
}

#[async_trait]
impl ConnectionPinger for PostgresConnectionPinger {
    async fn ping(&self) -> Result<(), CliError> {
        info!("Pinging Postgres at '{}'", &self.conn_str);

        // connect
        let (client, connection) = tokio_postgres::connect(&self.conn_str, NoTls)
            .await
            .map_err(|e| {
                error!("Postgres connection to '{}' failed: {}", &self.conn_str, e);
                CliError::Postgres(e)
            })?;

        // spawn the connection handler
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("Postgres connection error: {}", e);
            }
        });

        // run the simple query
        let row = client.query_one("SELECT 1", &[]).await.map_err(|e| {
            error!("Postgres ping query on '{}' failed: {}", &self.conn_str, e);
            CliError::Postgres(e)
        })?;

        // verify the result
        let val: i32 = row.get(0);
        if val != 1 {
            let msg = format!(
                "Postgres ping to '{}' returned unexpected result: {}",
                &self.conn_str, val
            );
            error!("{}", msg);
            return Err(CliError::Unexpected(msg));
        }

        info!("Postgres ping to '{}' succeeded", &self.conn_str);
        Ok(())
    }
}
