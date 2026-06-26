#![allow(dead_code)]

use connectors::error::DriverError;
use mysql_async::Pool;
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use std::sync::Arc;
use tokio_postgres::{Client, Config, NoTls, config::SslMode};
use tracing::{error, warn};

pub mod dag_integration;
pub mod integration;
pub mod paginate;
pub mod plugins;
pub mod resume;
pub mod schema_objects;
pub mod utils;
pub mod verify;
pub mod verify_schema_objects;

// Test database URLs
const TEST_MYSQL_URL_SAKILA: &str = "mysql://sakila_user:qwerty123@localhost:3306/sakila";
const TEST_MYSQL_URL_ORDERS: &str = "mysql://user:password@localhost:3306/testdb";
const TEST_PG_URL: &str = "postgres://user:password@localhost:5432/testdb";

pub(crate) async fn mysql_pool(source_db: &str) -> Pool {
    Pool::from_url(match source_db {
        "sakila" => TEST_MYSQL_URL_SAKILA,
        "orders" => TEST_MYSQL_URL_ORDERS,
        _ => panic!("Unknown source database: {source_db}"),
    })
    .expect("connect mysql")
}

async fn pg_pool() -> Arc<Client> {
    Arc::new(connect_client(TEST_PG_URL).await.expect("connect postgres"))
}

/// Drop & recreate the public schema in Postgres so it's empty.
/// Also clears the state store to ensure tests start with clean state.
async fn reset_postgres_schema() {
    let pool = pg_pool().await;
    // Drop and recreate public schema (removes all tables, types, etc.).
    pool.batch_execute(
        r#"
        DROP SCHEMA public CASCADE;
        CREATE SCHEMA public;
    "#,
    )
    .await
    .expect("reset postgres schema");

    // Clear the state store to prevent test pollution
    let home_dir = std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .ok();

    if let Some(home) = home_dir {
        let state_path = std::path::PathBuf::from(home).join(".stratum/state");
        if state_path.exists() {
            let _ = std::fs::remove_dir_all(&state_path);
        }
    }
}

async fn connect_client(url: &str) -> Result<Client, DriverError> {
    let config = url
        .parse::<Config>()
        .map_err(|e| DriverError::InvalidUrl(e.to_string()))?;
    let ssl_mode = config.get_ssl_mode();

    match ssl_mode {
        SslMode::Disable => connect_without_tls(config).await,
        SslMode::Require => connect_with_tls(config).await,
        SslMode::Prefer => match connect_with_tls(config.clone()).await {
            Ok(client) => Ok(client),
            Err(error) => {
                warn!(%error, "Postgres TLS handshake failed, retrying without TLS");
                connect_without_tls(config).await
            }
        },
        _ => connect_with_tls(config).await,
    }
}

async fn connect_with_tls(config: Config) -> Result<Client, DriverError> {
    let connector = TlsConnector::builder()
        .build()
        .map_err(|e| DriverError::ConnectionError(e.to_string()))?;
    let tls = MakeTlsConnector::new(connector);
    let (client, connection) = config.connect(tls).await?;
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            error!(%err, "Postgres connection error");
        }
    });
    Ok(client)
}

async fn connect_without_tls(config: Config) -> Result<Client, DriverError> {
    let (client, connection) = config.connect(NoTls).await?;
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            error!(%err, "Postgres connection error");
        }
    });
    Ok(client)
}
