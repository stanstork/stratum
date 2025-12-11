use engine_runtime::error::MigrationError;
use model::execution::errors::ConvertError;
use smql_syntax::errors::{BuildError, SmqlError};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CliError {
    #[error("Failed to read the configuration file: {0}")]
    ConfigFileRead(#[from] std::io::Error),

    #[error("Failed to parse the configuration file as SMQL: {0}")]
    ConfigParse(#[from] SmqlError),

    #[error("Failed to deserialize the configuration file as JSON AST: {0}")]
    ConfigDeserialize(#[from] serde_json::Error),

    #[error("Failed to run the migration plan: {0}")]
    Runner(#[from] MigrationError),

    #[error("Failed to serialize data to JSON: {0}")]
    JsonSerialize(serde_json::Error),

    #[error("Invalid connection format provided: {0}")]
    InvalidConnectionFormat(String),

    #[error("Unsupported connection kind for testing")]
    UnsupportedConnectionKind,

    /// MySQL driver error.
    #[error("MySQL error: {0}")]
    MySql(#[from] mysql_async::Error),

    /// PostgreSQL driver error.
    #[error("PostgreSQL error: {0}")]
    Postgres(#[from] tokio_postgres::Error),

    #[error("Unexpected error: {0}")]
    Unexpected(String),

    #[error("Migration error: {0}")]
    Migration(MigrationError),

    #[error("Shutdown requested")]
    ShutdownRequested,

    #[error("Plan build error: {0}")]
    PlanBuild(#[from] BuildError),

    #[error("Conversion error: {0}")]
    Conversion(#[from] ConvertError),

    #[error("Configuration error: {0}")]
    Config(String),
}
