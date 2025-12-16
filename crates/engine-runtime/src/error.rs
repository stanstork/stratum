use crate::dag::error::DagError;
use connectors::{error::AdapterError, sql::base::error::DbError};
use engine_config::settings::error::SettingsError;
use thiserror::Error;

/// Top‐level errors for the data migration engine.
#[derive(Debug, Error)]
pub enum MigrationError {
    /// Initialization error.
    #[error("Initialization error: {0}")]
    InitializationError(String),

    /// Adapter-related error.
    #[error("Adapter error: {0}")]
    Adapter(#[from] AdapterError),

    /// An unsupported data format was requested.
    #[error("Unsupported format: {0}")]
    UnsupportedFormat(String),

    /// An error occurred while joining a task.
    /// This usually indicates that the task was cancelled or panicked.
    #[error("Task join error: {0}")]
    TaskJoin(#[from] tokio::task::JoinError),

    #[error("Unexpected error: {0}")]
    Unexpected(String),

    #[error("Sled error: {0}")]
    Sled(#[from] sled::Error),

    /// Db error.
    #[error("Database error: {0}")]
    Database(#[from] DbError),

    /// Setting error.
    #[error("Settings error: {0}")]
    Settings(#[from] SettingsError),

    /// Unknown error.
    #[error("Unknown error: {0}")]
    Unknown(#[from] Box<dyn std::error::Error + Send + Sync>),

    /// Graceful shutdown was requested.
    #[error("Shutdown requested")]
    ShutdownRequested,

    /// Hook execution failed.
    #[error("Hook execution failed: {0}")]
    HookExecutionFailed(String),

    /// Pipeline execution failed.
    #[error("Pipeline failed: {0}")]
    PipelineFailed(String),

    /// Parallel pipelines failed.
    #[error("Parallel pipelines failed: {0:?}")]
    PipelinesFailed(Vec<String>),

    /// DAG error.
    #[error("DAG error: {0}")]
    Dag(#[from] DagError),
}

/// Common error type for all actors in the engine.
#[derive(Debug, Error)]
pub enum ActorError {
    #[error("Mailbox closed")]
    MailboxClosed,

    #[error("Actor internal error: {0}")]
    Internal(String),
}
