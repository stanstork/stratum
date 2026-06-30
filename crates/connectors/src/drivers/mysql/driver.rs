use crate::{
    drivers::mysql::tls,
    error::DriverError,
    sql::metadata::capabilities::Capabilities,
    traits::driver::{Driver, DriverInfo},
};
use mysql_async::{Pool, prelude::Queryable};
use tracing::info;

const MYSQL_MAX_PREPARED_STMT_PARAMS: u16 = 65535;

#[derive(Clone)]
pub struct MySqlDriver {
    pool: Pool,
    capabilities: Capabilities,
}

impl MySqlDriver {
    /// Static driver info for registration
    pub const INFO: DriverInfo = DriverInfo {
        id: "mysql",
        name: "MySQL",
        schemes: &["mysql", "mariadb"],
    };

    /// Establishes a connection pool and detects server capabilities.
    pub async fn connect(url: &str) -> Result<Self, DriverError> {
        let pool = tls::pool_from_url(url)?;
        let capabilities = Self::detect_capabilities(&pool).await?;

        info!(driver = "mysql", "database connection established");

        Ok(Self { pool, capabilities })
    }

    pub fn pool(&self) -> &Pool {
        &self.pool
    }

    /// Fetches the version string from the DB and resolves capabilities.
    async fn detect_capabilities(pool: &Pool) -> Result<Capabilities, DriverError> {
        let mut conn = pool
            .get_conn()
            .await
            .map_err(|e| DriverError::ConnectionError(e.to_string()))?;

        let version: String = conn
            .query_first("SELECT VERSION()")
            .await
            .map_err(|e| DriverError::QueryError(e.to_string()))?
            .ok_or_else(|| DriverError::QueryError("Failed to retrieve database version".into()))?;

        // Drop connection explicitly or let it drop out of scope;
        // we're done with I/O here.
        drop(conn);

        Ok(Self::resolve_capabilities(version))
    }

    fn resolve_capabilities(version: String) -> Capabilities {
        let version_lower = version.to_lowercase();
        let is_mariadb = version_lower.contains("mariadb");

        // TODO: RETURNING is supported in MariaDB 10.5+ (INSERT) and 10.0+ (DELETE).
        // For accurate support, a SemVer parser would be ideal here.
        let supports_returning = is_mariadb;

        Capabilities {
            version,
            transactions: true,
            savepoints: true,
            copy_protocol: true, // Corresponds to LOAD DATA LOCAL INFILE
            upsert: true,        // ON DUPLICATE KEY UPDATE
            returning_clause: supports_returning,
            json_type: true,   // Supported in MySQL 5.7+ and MariaDB 10.2+ (as alias)
            jsonb_type: false, // MySQL has JSON, but not a distinct JSONB binary type like PG
            array_type: false,
            uuid_type: false, // Usually stored as BINARY(16) or CHAR(36)
            geometry_type: true,
            max_parameters: Some(MYSQL_MAX_PREPARED_STMT_PARAMS.into()),
            max_query_size: None, // Depends on server's max_allowed_packet, usually dynamic
        }
    }
}

impl Driver for MySqlDriver {
    fn info(&self) -> &DriverInfo {
        &Self::INFO
    }

    fn version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }

    fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }
}
