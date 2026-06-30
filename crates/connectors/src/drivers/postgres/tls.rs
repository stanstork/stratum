use crate::error::DriverError;
use native_tls::{Certificate, TlsConnector};
use postgres_native_tls::MakeTlsConnector;
use tokio_postgres::{Client, Config, NoTls};
use tracing::{error, warn};

/// Certificate-verification policy derived from the URL's `sslmode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SslPolicy {
    /// No TLS.
    Disable,
    /// Use TLS if the server offers it, otherwise fall back to plaintext.
    Prefer,
    /// Require TLS, but do not verify the certificate (libpq `require`).
    Require,
    /// Require TLS and verify the certificate chain (but not the hostname).
    VerifyCa,
    /// Require TLS and verify both the certificate chain and the hostname.
    VerifyFull,
}

impl SslPolicy {
    fn parse(value: &str) -> Result<Self, DriverError> {
        match value.to_ascii_lowercase().as_str() {
            "disable" => Ok(Self::Disable),
            // `allow` is opportunistic like `prefer` from the client's side.
            "allow" | "prefer" => Ok(Self::Prefer),
            "require" => Ok(Self::Require),
            "verify-ca" => Ok(Self::VerifyCa),
            "verify-full" => Ok(Self::VerifyFull),
            other => Err(DriverError::InvalidUrl(format!(
                "unknown sslmode '{other}' (expected disable, prefer, require, verify-ca or verify-full)"
            ))),
        }
    }

    /// The `sslmode` value `tokio_postgres::Config` accepts for this policy.
    fn tokio_sslmode(self) -> &'static str {
        match self {
            Self::Disable => "disable",
            Self::Prefer => "prefer",
            Self::Require | Self::VerifyCa | Self::VerifyFull => "require",
        }
    }
}

/// Connect to PostgreSQL.
pub(crate) async fn connect(url: &str) -> Result<Client, DriverError> {
    let (config, policy, ca_path) = build_config(url)?;

    match policy {
        SslPolicy::Disable => connect_no_tls(&config).await,
        SslPolicy::Prefer => {
            // Opportunistic: attempt TLS without verification, fall back to
            // plaintext if the handshake fails or the server has no SSL.
            let connector = build_connector(true, true, ca_path.as_deref())?;
            match connect_tls(&config, connector).await {
                Ok(client) => Ok(client),
                Err(error) => {
                    warn!(%error, "postgres TLS handshake failed, retrying without TLS");
                    connect_no_tls(&config).await
                }
            }
        }
        SslPolicy::Require => {
            // libpq `require`: encrypt, but do not authenticate the server.
            let connector = build_connector(true, true, ca_path.as_deref())?;
            connect_tls(&config, connector).await
        }
        SslPolicy::VerifyCa => {
            // Verify the certificate chain, but not the hostname.
            let connector = build_connector(false, true, ca_path.as_deref())?;
            connect_tls(&config, connector).await
        }
        SslPolicy::VerifyFull => {
            // Verify both the certificate chain and the hostname.
            let connector = build_connector(false, false, ca_path.as_deref())?;
            connect_tls(&config, connector).await
        }
    }
}

/// Parse the URL, splitting off the libpq-specific TLS parameters
/// (`sslmode`, `sslrootcert`) that `tokio_postgres::Config` cannot handle, and
/// return a `Config` with a normalized `sslmode`.
fn build_config(url: &str) -> Result<(Config, SslPolicy, Option<String>), DriverError> {
    let (normalized, policy, ca_path) = normalize_url(url)?;
    let config: Config = normalized
        .parse()
        .map_err(|e: tokio_postgres::Error| DriverError::InvalidUrl(e.to_string()))?;
    Ok((config, policy, ca_path))
}

/// Pure URL surgery: extract the `sslmode`/`sslrootcert` parameters, and return
/// a URL whose `sslmode` is one `tokio_postgres::Config` accepts.
fn normalize_url(url: &str) -> Result<(String, SslPolicy, Option<String>), DriverError> {
    let mut parsed = url::Url::parse(url).map_err(|e| DriverError::InvalidUrl(e.to_string()))?;

    let mut sslmode: Option<String> = None;
    let mut sslrootcert: Option<String> = None;
    let kept: Vec<(String, String)> = parsed
        .query_pairs()
        .filter_map(|(k, v)| match k.as_ref() {
            "sslmode" => {
                sslmode = Some(v.into_owned());
                None
            }
            "sslrootcert" => {
                sslrootcert = Some(v.into_owned());
                None
            }
            _ => Some((k.into_owned(), v.into_owned())),
        })
        .collect();

    let policy = match sslmode {
        Some(ref mode) => SslPolicy::parse(mode)?,
        // `tokio_postgres` defaults to Prefer when `sslmode` is absent.
        None => SslPolicy::Prefer,
    };

    // Rebuild the query string without the libpq-only TLS params, and with a
    // `sslmode` that `tokio_postgres` understands.
    {
        let mut pairs = parsed.query_pairs_mut();
        pairs.clear();
        for (k, v) in &kept {
            pairs.append_pair(k, v);
        }
        pairs.append_pair("sslmode", policy.tokio_sslmode());
    }

    Ok((parsed.to_string(), policy, sslrootcert))
}

fn build_connector(
    accept_invalid_certs: bool,
    accept_invalid_hostnames: bool,
    ca_path: Option<&str>,
) -> Result<MakeTlsConnector, DriverError> {
    let mut builder = TlsConnector::builder();
    builder.danger_accept_invalid_certs(accept_invalid_certs);
    builder.danger_accept_invalid_hostnames(accept_invalid_hostnames);

    if let Some(path) = ca_path {
        let pem = std::fs::read(path).map_err(|e| {
            DriverError::ConnectionError(format!("failed to read sslrootcert '{path}': {e}"))
        })?;
        let cert = Certificate::from_pem(&pem).map_err(|e| {
            DriverError::ConnectionError(format!("invalid sslrootcert '{path}': {e}"))
        })?;
        builder.add_root_certificate(cert);
    }

    let connector = builder
        .build()
        .map_err(|e| DriverError::ConnectionError(e.to_string()))?;
    Ok(MakeTlsConnector::new(connector))
}

async fn connect_tls(config: &Config, tls: MakeTlsConnector) -> Result<Client, DriverError> {
    let (client, connection) = config
        .connect(tls)
        .await
        .map_err(|e| DriverError::ConnectionError(e.to_string()))?;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            error!(%error, "postgres connection error");
        }
    });
    Ok(client)
}

async fn connect_no_tls(config: &Config) -> Result<Client, DriverError> {
    let (client, connection) = config
        .connect(NoTls)
        .await
        .map_err(|e| DriverError::ConnectionError(e.to_string()))?;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            error!(%error, "postgres connection error");
        }
    });
    Ok(client)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn query_params(url: &str) -> std::collections::HashMap<String, String> {
        url::Url::parse(url)
            .unwrap()
            .query_pairs()
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect()
    }

    #[test]
    fn defaults_to_prefer_when_sslmode_absent() {
        let (out, policy, ca) = normalize_url("postgres://u:p@host:5432/db").unwrap();
        assert_eq!(policy, SslPolicy::Prefer);
        assert_eq!(ca, None);
        assert_eq!(
            query_params(&out).get("sslmode").map(String::as_str),
            Some("prefer")
        );
    }

    #[test]
    fn disable_is_preserved() {
        let (out, policy, _) = normalize_url("postgres://u:p@host/db?sslmode=disable").unwrap();
        assert_eq!(policy, SslPolicy::Disable);
        assert_eq!(
            query_params(&out).get("sslmode").map(String::as_str),
            Some("disable")
        );
    }

    #[test]
    fn verify_full_is_normalized_to_require_for_tokio() {
        // tokio_postgres only understands disable/prefer/require, so the
        // stricter modes must be downgraded in the URL while the policy is kept.
        let (out, policy, _) = normalize_url("postgres://u:p@host/db?sslmode=verify-full").unwrap();
        assert_eq!(policy, SslPolicy::VerifyFull);
        assert_eq!(
            query_params(&out).get("sslmode").map(String::as_str),
            Some("require")
        );
    }

    #[test]
    fn sslrootcert_is_extracted_and_stripped() {
        let (out, policy, ca) =
            normalize_url("postgres://u:p@host/db?sslmode=verify-ca&sslrootcert=/tmp/ca.pem")
                .unwrap();
        assert_eq!(policy, SslPolicy::VerifyCa);
        assert_eq!(ca.as_deref(), Some("/tmp/ca.pem"));
        let params = query_params(&out);
        assert!(
            !params.contains_key("sslrootcert"),
            "sslrootcert must be stripped"
        );
        assert_eq!(params.get("sslmode").map(String::as_str), Some("require"));
    }

    #[test]
    fn other_query_params_are_preserved() {
        let (out, _, _) =
            normalize_url("postgres://u:p@host/db?application_name=stratum&sslmode=require")
                .unwrap();
        let params = query_params(&out);
        assert_eq!(
            params.get("application_name").map(String::as_str),
            Some("stratum")
        );
        assert_eq!(params.get("sslmode").map(String::as_str), Some("require"));
    }

    #[test]
    fn unknown_sslmode_is_rejected() {
        let err = normalize_url("postgres://u:p@host/db?sslmode=bogus").unwrap_err();
        assert!(matches!(err, DriverError::InvalidUrl(_)));
    }
}
