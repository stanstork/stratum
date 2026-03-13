use crate::io::error::SinkError;
use connectors::error::DriverError;
use engine_state::error::StateStoreError;
use model::pagination::cursor::Cursor;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConsumerError {
    #[error("Failed to write batch '{batch_id}': {source}")]
    Write {
        batch_id: String,
        #[source]
        source: SinkError,
    },

    #[error("Failed to load consumer state: {0}")]
    StateLoad(#[from] StateStoreError),

    #[error("Failed to save checkpoint for batch '{batch_id}': {source}")]
    Checkpoint {
        batch_id: String,
        #[source]
        source: StateStoreError,
    },

    #[error("Failed to toggle triggers for table '{table}': {source}")]
    ToggleTrigger {
        table: String,
        #[source]
        source: DriverError,
    },

    #[error("Failed to deserialize record: {0}")]
    Deserialization(String),

    #[error("Sink error: {0}")]
    Sink(#[from] SinkError),

    #[error("Retry attempts exhausted: {0}")]
    RetriesExhausted(String),

    #[error("Circuit breaker opened for stage '{stage}': {last_error}")]
    CircuitBreakerOpen { stage: String, last_error: String },
}

#[derive(Error, Debug)]
pub enum ProducerError {
    #[error("State store operation failed: {0}")]
    StateStore(#[from] StateStoreError),

    #[error("Fetch failed at cursor {cursor:?}: {source}")]
    Fetch {
        cursor: Cursor,
        #[source]
        source: DriverError,
    },

    #[error("Failed to store data in the buffer: {0}")]
    Store(String),

    #[error("Failed to store offset in the buffer: {0}")]
    StoreOffset(String),

    #[error("The consumer channel was closed.")]
    ChannelClosed,

    #[error("Other error: {0}")]
    Other(String),

    #[error("Driver error: {0}")]
    Driver(#[from] DriverError),

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
            // Non-fatal transformation errors (bad data) should NOT stop migration
            ProducerError::Transform(transform_err) => transform_err.is_fatal(),
            // All other errors go through circuit breaker (transient external system failures)
            _ => false,
        }
    }

    /// Returns true if this is a graceful shutdown signal (not an error, just cleanup).
    pub fn is_shutdown(&self) -> bool {
        matches!(self, ProducerError::ChannelClosed)
    }
}
