use thiserror::Error;

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
