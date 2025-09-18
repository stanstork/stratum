use crate::error::MigrationError;
use async_trait::async_trait;
use smql::statements::connection::DataFormat;
use sql_adapter::error::db::DbError;
use sqlx::{MySql, Pool};
use sqlx::{Postgres, Row};
use std::str::FromStr;
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
            other => Err(format!("Unknown connection kind: {}", other)),
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
    async fn ping(&self) -> Result<(), MigrationError>;
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
    async fn ping(&self) -> Result<(), MigrationError> {
        info!("Pinging MySQL at '{}'", &self.conn_str);

        // connect
        let pool = Pool::<MySql>::connect(&self.conn_str).await.map_err(|e| {
            error!("MySQL connection to '{}' failed: {}", &self.conn_str, e);
            MigrationError::from(DbError::from(e))
        })?;

        // run the simple query
        let row = sqlx::query("SELECT 1")
            .fetch_one(&pool)
            .await
            .map_err(|e| {
                error!("MySQL ping query on '{}' failed: {}", &self.conn_str, e);
                MigrationError::from(DbError::from(e))
            })?;

        // verify the result
        let val: i32 = row.get(0);
        if val != 1 {
            let msg = format!(
                "MySQL ping to '{}' returned unexpected result: {}",
                &self.conn_str, val
            );
            error!("{}", msg);
            return Err(MigrationError::Unexpected(msg));
        }

        info!("MySQL ping to '{}' succeeded", &self.conn_str);
        Ok(())
    }
}

#[async_trait]
impl ConnectionPinger for PostgresConnectionPinger {
    async fn ping(&self) -> Result<(), MigrationError> {
        info!("Pinging Postgres at '{}'", &self.conn_str);

        // connect
        let pool = Pool::<Postgres>::connect(&self.conn_str)
            .await
            .map_err(|e| {
                error!("Postgres connection to '{}' failed: {}", &self.conn_str, e);
                MigrationError::from(DbError::from(e))
            })?;

        // run the simple query
        let row = sqlx::query("SELECT 1")
            .fetch_one(&pool)
            .await
            .map_err(|e| {
                error!("Postgres ping query on '{}' failed: {}", &self.conn_str, e);
                MigrationError::from(DbError::from(e))
            })?;

        // verify the result
        let val: i32 = row.get(0);
        if val != 1 {
            let msg = format!(
                "Postgres ping to '{}' returned unexpected result: {}",
                &self.conn_str, val
            );
            error!("{}", msg);
            return Err(MigrationError::Unexpected(msg));
        }

        info!("Postgres ping to '{}' succeeded", &self.conn_str);
        Ok(())
    }
}
