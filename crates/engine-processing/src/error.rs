use engine_core::error::SinkError;
use model::pagination::cursor::Cursor;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConsumerError {
    #[error("Failed to write batch to destination for table '{table}': {source}")]
    WriteBatch {
        table: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("Failed to write batch '{batch_id}': {source}")]
    Write {
        batch_id: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("Failed to load consumer state: {source}")]
    StateLoad {
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("Failed to save checkpoint for batch '{batch_id}': {source}")]
    Checkpoint {
        batch_id: String,
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

    #[error("Unexpected error: {0}")]
    Unexpected(#[from] Box<dyn std::error::Error + Send + Sync>),

    #[error("Sink error: {0}")]
    Sink(#[from] SinkError),

    #[error("Retry attempts exhausted: {0}")]
    RetriesExhausted(String),

    #[error("Circuit breaker opened for stage '{stage}': {last_error}")]
    CircuitBreakerOpen { stage: String, last_error: String },
}

#[derive(Error, Debug)]
pub enum ProducerError {
    #[error("State store operation failed: {source}")]
    StateStore {
        #[from]
        source: StateError,
    },

    #[error("Fetch failed at cursor {cursor:?}: {source}")]
    Fetch {
        cursor: Cursor,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("Failed to store data in the buffer: {0}")]
    Store(String),

    #[error("Failed to store offset in the buffer: {0}")]
    StoreOffset(String),

    #[error("The consumer channel was closed.")]
    ChannelClosed,

    #[error("Other error: {0}")]
    Other(String),

    #[error("Unexpected error: {0}")]
    Unexpected(#[from] Box<dyn std::error::Error + Send + Sync>),

    #[error("Retry attempts exhausted: {0}")]
    RetriesExhausted(String),

    #[error("Circuit breaker opened for stage '{stage}': {last_error}")]
    CircuitBreakerOpen { stage: String, last_error: String },

    #[error("Transformation failed: {0}")]
    Transform(#[from] crate::transform::error::TransformError),

    #[error("Producer finished work.")]
    Finished,
}

impl ProducerError {
    /// Returns true if this error should bypass the circuit breaker and stop immediately.
    /// Circuit breakers are for external system failures (transient), not business logic errors (permanent).
    pub fn is_fatal(&self) -> bool {
        match self {
            // Transformation errors are permanent business logic errors - stop immediately
            ProducerError::Transform(_) => true,
            // All other errors go through circuit breaker (transient external system failures)
            _ => false,
        }
    }

    /// Returns true if this is a graceful shutdown signal (not an error, just cleanup).
    pub fn is_shutdown(&self) -> bool {
        matches!(self, ProducerError::ChannelClosed)
    }
}

#[derive(Debug, Error)]
pub enum StateError {
    #[error("Checkpoint load failed: {0}")]
    CheckpointLoad(String),

    #[error("WAL operation failed: {0}")]
    WalOperation(String),

    #[error("Serialization error: {0}")]
    Serialization(String),
}
