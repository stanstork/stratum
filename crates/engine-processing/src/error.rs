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

    #[error("The consumer channel was closed unexpectedly.")]
    ShutdownSignal,

    #[error("Other error: {0}")]
    Other(String),

    #[error("Unexpected error: {0}")]
    Unexpected(#[from] Box<dyn std::error::Error + Send + Sync>),

    #[error("Failed to send batch: {0}")]
    ChannelSend(String),

    #[error("Retry attempts exhausted: {0}")]
    RetriesExhausted(String),

    #[error("Circuit breaker opened for stage '{stage}': {last_error}")]
    CircuitBreakerOpen { stage: String, last_error: String },

    #[error("Producer finished work.")]
    Finished,
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

#[derive(Error, Debug)]
pub enum TransformError {
    #[error("Transformation failed: {0}")]
    Transformation(String),

    #[error("Validation failed: {rule} - {message}")]
    ValidationFailed { rule: String, message: String },
}
