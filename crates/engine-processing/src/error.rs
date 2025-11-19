use connectors::error::AdapterError;
use engine_core::error::SinkError;
use thiserror::Error;

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
    #[error("Failed to fetch data: {0}")]
    Fetch(#[from] AdapterError),

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

    #[error("State store error: {0}")]
    StateStore(String),

    #[error("Failed to send batch: {0}")]
    ChannelSend(String),

    #[error("Retry attempts exhausted: {0}")]
    RetriesExhausted(String),

    #[error("Circuit breaker opened for stage '{stage}': {last_error}")]
    CircuitBreakerOpen { stage: String, last_error: String },
}
