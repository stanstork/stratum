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

        // `LOAD DATA LOCAL INFILE` (fast-path bulk load) only works when the
        // server has `local_infile` enabled. `@@GLOBAL.local_infile` returns 0/1.
        let local_infile_enabled = conn
            .query_first::<i64, _>("SELECT @@GLOBAL.local_infile")
            .await
            .map_err(|e| DriverError::QueryError(e.to_string()))?
            .map(|v| v != 0)
            .unwrap_or(false);

        // Drop connection explicitly or let it drop out of scope;
        // we're done with I/O here.
        drop(conn);

        Ok(Self::resolve_capabilities(version, local_infile_enabled))
    }

    fn resolve_capabilities(version: String, local_infile_enabled: bool) -> Capabilities {
        // MySQL has no `RETURNING`. MariaDB added `INSERT ... RETURNING` in 10.5;
        // since our write path uses INSERT, gate the capability on that version.
        let supports_returning = Self::mariadb_supports_returning(&version);

        Capabilities {
            version,
            transactions: true,
            savepoints: true,
            copy_protocol: local_infile_enabled, // LOAD DATA LOCAL INFILE requires server `local_infile=1`
            upsert: true,                        // ON DUPLICATE KEY UPDATE
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

    /// Returns `true` when the version string denotes MariaDB 10.5 or newer,
    /// which is when `INSERT ... RETURNING` became available.
    fn mariadb_supports_returning(version: &str) -> bool {
        if !version.to_lowercase().contains("mariadb") {
            return false;
        }

        // Strip the legacy `5.5.5-` compatibility prefix if present.
        let core = version.strip_prefix("5.5.5-").unwrap_or(version);

        // Parse the leading `major.minor` from the remaining string.
        let mut parts = core.split('.');
        let major: u32 = match parts.next().and_then(|p| p.parse().ok()) {
            Some(v) => v,
            None => return false,
        };
        // The minor segment may carry a suffix, e.g. `6-MariaDB`.
        let minor: u32 = match parts.next().map(|p| {
            p.chars()
                .take_while(|c| c.is_ascii_digit())
                .collect::<String>()
                .parse()
                .unwrap_or(0)
        }) {
            Some(v) => v,
            None => return false,
        };

        (major, minor) >= (10, 5)
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
