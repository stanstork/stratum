use engine::error::MigrationError;
use smql::error::SmqlError;
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
}
