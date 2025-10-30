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
