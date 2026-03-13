use engine_planner::builder::errors::{ConnectionError, ReportBuilderError};
use engine_runtime::{dag::error::DagError, error::MigrationError};
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

    #[error(
        "Config file not found. Searched in the following locations:\n{0}\nPlease specify a config file with --config or create one in a standard location."
    )]
    ConfigNotFound(String),

    #[error("DAG error: {0}")]
    Dag(#[from] DagError),

    #[error("Report builder error: {0}")]
    ReportBuilder(#[from] ReportBuilderError),

    #[error("Connection test failed: {0}")]
    Connection(#[from] ConnectionError),

    #[error("Unknown error: {0}")]
    Unknown(String),
}
