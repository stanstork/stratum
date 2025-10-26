use core::error;

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

    #[error("Report generation error: {0}")]
    Report(#[from] ReportGenerationError),

    #[error("Sled error: {0}")]
    Sled(#[from] sled::Error),
}

#[derive(Error, Debug)]
pub enum ConsumerError {
    #[error("Failed to write batch to destination for table '{table}': {source}")]
    WriteBatch {
        table: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("Failed to toggle triggers for table '{table}': {source}")]
    ToggleTrigger {
        table: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("Failed to deserialize record: {0}")]
    Deserialization(String),
}

#[derive(Error, Debug)]
pub enum ProducerError {
    #[error("Failed to fetch data: {source}")]
    Fetch {
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("Failed to store data in the buffer: {0}")]
    Store(String),

    #[error("Failed to store offset in the buffer: {0}")]
    StoreOffset(String),

    #[error("The consumer channel was closed unexpectedly.")]
    ShutdownSignal,

    #[error("Other error: {0}")]
    Other(String),
}

#[derive(Error, Debug)]
pub enum ReportGenerationError {
    #[error("Failed to determine source endpoint type")]
    SourceEndpointError,

    #[error("Failed to determine destination endpoint type")]
    DestinationEndpointError,
}

#[derive(Error, Debug)]
pub enum WallEntryError {
    #[error("Failed to deserialize WAL entry: {0}")]
    Deserialization(String),

    #[error("Other error: {0}")]
    Other(String),
}

#[derive(Error, Debug)]
pub enum StateStoreError {
    #[error("Failed to save checkpoint: {0}")]
    SaveCheckpoint(String),

    #[error("Failed to load checkpoint: {0}")]
    LoadCheckpoint(String),

    #[error("Failed to append WAL entry: {0}")]
    AppendWAL(String),

    #[error("Failed to iterate WAL entries: {0}")]
    IterateWAL(String),
}
