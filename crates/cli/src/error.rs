use engine_planner::builder::errors::{ConnectionError, ReportBuilderError};
use engine_runtime::{dag::error::DagError, error::MigrationError};
use engine_verify::error::VerifyError;
use engine_wasm::error::WasmError;
use model::execution::errors::ConvertError;
use smql_syntax::errors::{BuildError, SmqlError};
use stratum_plugin_compiler::CompileError;
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

    #[error("Verification error: {0}")]
    Verification(#[from] VerifyError),

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

    #[error("Migration paused - resume with the same config to continue")]
    Paused,

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

    #[error("Plugin compile error: {0}")]
    PluginCompile(#[from] CompileError),

    #[error("Plugin error: {0}")]
    Wasm(#[from] WasmError),

    /// Non-error user-facing message (prints to stderr, exits with code 1, no ERROR log)
    #[error("{0}")]
    UserMessage(String),

    #[error("Unknown error: {0}")]
    Unknown(String),
}
