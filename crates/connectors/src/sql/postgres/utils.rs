use crate::sql::base::error::ConnectorError;
use model::core::value::Value;
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use tokio_postgres::{Client, Config, NoTls, config::SslMode};
use tracing::{error, warn};

pub(crate) async fn connect_client(url: &str) -> Result<Client, ConnectorError> {
    let config = url
        .parse::<Config>()
        .map_err(|e| ConnectorError::InvalidUrl(e.to_string()))?;
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

pub(crate) async fn connect_with_tls(config: Config) -> Result<Client, ConnectorError> {
    let connector = TlsConnector::builder().build()?;
    let tls = MakeTlsConnector::new(connector);
    let (client, connection) = config.connect(tls).await?;
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            error!(%err, "Postgres connection error");
        }
    });
    Ok(client)
}

pub(crate) async fn connect_without_tls(config: Config) -> Result<Client, ConnectorError> {
    let (client, connection) = config.connect(NoTls).await?;
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            error!(%err, "Postgres connection error");
        }
    });
    Ok(client)
}

pub(crate) fn flatten_values(batches: &[Vec<Value>]) -> Vec<Value> {
    let total = batches.iter().map(|batch| batch.len()).sum();
    let mut flat = Vec::with_capacity(total);
    for batch in batches {
        flat.extend(batch.iter().cloned());
    }
    flat
}
