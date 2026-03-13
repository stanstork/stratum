use thiserror::Error;

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

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Storage error: {0}")]
    Storage(String),
}
