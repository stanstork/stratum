use super::{queries::escape_identifier, tls};
use crate::{
    error::DriverError,
    sql::metadata::capabilities::Capabilities,
    traits::driver::{Driver, DriverInfo},
};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_postgres::Client;
use tracing::info;

const PG_MAX_PREPARED_STMT_PARAMS: usize = 65535;

/// Default schema when a connection doesn't specify one.
pub const DEFAULT_SCHEMA: &str = "public";

#[derive(Clone)]
pub struct PgDriver {
    client: Arc<RwLock<Client>>,
    url: String,
    schema: String,
    capabilities: Capabilities,
}

impl PgDriver {
    /// Static driver info for registration
    pub const INFO: DriverInfo = DriverInfo {
        id: "postgres",
        name: "PostgreSQL",
        schemes: &["postgres", "postgresql"],
    };

    /// Establishes a connection (schema `public`) and detects server capabilities.
    pub async fn connect(url: &str) -> Result<Self, DriverError> {
        Self::connect_with_schema(url, DEFAULT_SCHEMA).await
    }

    /// Establishes a connection scoped to `schema`. The session `search_path` is
    /// set so that unqualified reads, writes, and DDL target that schema, and
    /// the schema is used to scope introspection queries.
    pub async fn connect_with_schema(url: &str, schema: &str) -> Result<Self, DriverError> {
        let client = tls::connect(url).await?;
        set_search_path(&client, schema).await?;

        let client = Arc::new(RwLock::new(client));
        let capabilities = Self::detect_capabilities(&client).await?;

        info!(
            driver = "postgres",
            schema, "database connection established"
        );

        Ok(Self {
            client,
            url: url.to_string(),
            schema: schema.to_string(),
            capabilities,
        })
    }

    pub fn client(&self) -> &Arc<RwLock<Client>> {
        &self.client
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn schema(&self) -> &str {
        &self.schema
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

/// Set the session `search_path` so unqualified names resolve to `schema`.
pub(crate) async fn set_search_path(client: &Client, schema: &str) -> Result<(), DriverError> {
    if schema == DEFAULT_SCHEMA {
        return Ok(());
    }

    let sql = format!(
        "SET search_path TO {}, {}",
        escape_identifier(schema),
        escape_identifier(DEFAULT_SCHEMA)
    );

    client
        .batch_execute(&sql)
        .await
        .map_err(|e| DriverError::QueryError(e.to_string()))
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
