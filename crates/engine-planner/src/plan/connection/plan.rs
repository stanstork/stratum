use crate::plan::connection::{
    pool::PoolConfig,
    status::{ConnectionRole, ConnectionStatus},
};
use serde::Serialize;

#[derive(Serialize, Debug, Clone)]
pub struct ConnectionPlan {
    pub name: String,
    pub driver: DatabaseDriver,
    pub url_masked: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub pool: Option<PoolConfig>,

    pub status: ConnectionStatus,
    pub role: ConnectionRole,
}

#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum DatabaseDriver {
    Postgres,
    MySql,
    Sqlite,
    Mssql,
    Oracle,
    Other(String),
}

impl DatabaseDriver {
    pub fn display_name(&self) -> &str {
        match self {
            DatabaseDriver::Postgres => "PostgreSQL",
            DatabaseDriver::MySql => "MySQL",
            DatabaseDriver::Sqlite => "SQLite",
            DatabaseDriver::Mssql => "SQL Server",
            DatabaseDriver::Oracle => "Oracle",
            DatabaseDriver::Other(s) => s,
        }
    }

    pub fn from_name(driver: &str) -> Self {
        match driver.to_lowercase().as_str() {
            "postgres" | "postgresql" => DatabaseDriver::Postgres,
            "mysql" => DatabaseDriver::MySql,
            "sqlite" | "sqlite3" => DatabaseDriver::Sqlite,
            "mssql" | "sqlserver" => DatabaseDriver::Mssql,
            "oracle" => DatabaseDriver::Oracle,
            other => DatabaseDriver::Other(other.to_string()),
        }
    }
}
