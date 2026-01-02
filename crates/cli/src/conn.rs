use std::str::FromStr;

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
    /// Detect connection kind from URL scheme
    pub fn from_url(url: &str) -> Result<Self, String> {
        if url.starts_with("mysql://") || url.starts_with("mariadb://") {
            Ok(ConnectionKind::MySql)
        } else if url.starts_with("postgresql://")
            || url.starts_with("postgres://")
            || url.starts_with("pg://")
        {
            Ok(ConnectionKind::Postgres)
        } else if url.starts_with("ftp://") || url.starts_with("ftps://") {
            Ok(ConnectionKind::Ftp)
        } else {
            Err("Cannot detect connection type from URL. Supported schemes: mysql://, postgresql://, postgres://, ftp://".to_string())
        }
    }
}
