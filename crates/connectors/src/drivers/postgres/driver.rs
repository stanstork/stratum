use crate::{
    error::DriverError,
    sql::metadata::capabilities::Capabilities,
    traits::driver::{Driver, DriverInfo},
};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_postgres::{Client, NoTls};

const PG_MAX_PREPARED_STMT_PARAMS: usize = 65535;

#[derive(Clone)]
pub struct PgDriver {
    client: Arc<RwLock<Client>>,
    url: String,
    capabilities: Capabilities,
}

impl PgDriver {
    /// Static driver info for registration
    pub const INFO: DriverInfo = DriverInfo {
        id: "postgres",
        name: "PostgreSQL",
        schemes: &["postgres", "postgresql"],
    };

    /// Establishes a connection and detects server capabilities.
    pub async fn connect(url: &str) -> Result<Self, DriverError> {
        let (client, connection) = tokio_postgres::connect(url, NoTls)
            .await
            .map_err(|e| DriverError::ConnectionError(e.to_string()))?;

        // Spawn connection handler
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!("PostgreSQL connection error: {}", e);
            }
        });

        let client = Arc::new(RwLock::new(client));
        let capabilities = Self::detect_capabilities(&client).await?;

        Ok(Self {
            client,
            url: url.to_string(),
            capabilities,
        })
    }

    pub fn client(&self) -> &Arc<RwLock<Client>> {
        &self.client
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    /// Fetches the version string from the DB and resolves capabilities.
    async fn detect_capabilities(
        client: &Arc<RwLock<Client>>,
    ) -> Result<Capabilities, DriverError> {
        let client = client.read().await;

        let row = client
            .query_one("SELECT version()", &[])
            .await
            .map_err(|e| DriverError::QueryError(e.to_string()))?;

        let version: String = row.get(0);

        Ok(Self::resolve_capabilities(version))
    }

    fn resolve_capabilities(version: String) -> Capabilities {
        Capabilities {
            version,
            transactions: true,
            savepoints: true,
            copy_protocol: true,
            upsert: true,
            returning_clause: true,
            json_type: true,
            jsonb_type: true,
            array_type: true,
            uuid_type: true,
            geometry_type: true,
            max_parameters: Some(PG_MAX_PREPARED_STMT_PARAMS),
            max_query_size: None,
        }
    }
}

impl Driver for PgDriver {
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
