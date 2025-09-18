use crate::settings::error::SettingsError;
use csv::error::FileError;
use sql_adapter::error::{adapter::ConnectorError, db::DbError};
use thiserror::Error;

/// Top‐level errors for the data migration engine.
#[derive(Debug, Error)]
pub enum MigrationError {
    /// Failed to initialize a data connector/adapter.
    #[error("Connector error: {0}")]
    Connector(#[from] ConnectorError),

    // Any error coming from the database layer.
    #[error("Database error: {0}")]
    Database(#[from] DbError),

    /// An unsupported data format was requested.
    #[error("Unsupported format: {0}")]
    UnsupportedFormat(String),

    /// Something went wrong applying migration settings.
    #[error("Settings application error: {0}")]
    Settings(#[from] SettingsError),

    /// An error occurred while joining a task.
    /// This usually indicates that the task was cancelled or panicked.
    #[error("Task join error: {0}")]
    TaskJoin(#[from] tokio::task::JoinError),

    /// Error occurred while retrieving the adapter from the context for the database source.
    #[error("Adapter not found: {0}")]
    AdapterNotFound(String),

    #[error("File error: {0}")]
    FileError(#[from] FileError),

    #[error("Invalid metadata: {0}")]
    InvalidMetadata(String),

    #[error("Missing REPORT_CALLBACK_URL environment variable")]
    MissingCallbackUrl,

    #[error("Failed to send report")]
    ReportFailed,

    #[error("Missing AUTH_TOKEN environment variable")]
    MissingAuthToken,

    #[error("Unexpected error: {0}")]
    Unexpected(String),
}
