use connectors::sql::base::error::DbError;
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

#[derive(Error, Debug)]
pub enum SinkError {
    #[error("IO Error: {0}")]
    Io(String),

    #[error("Protocol Error: {0}")]
    Protocol(String),

    #[error("Closed Sink")]
    Closed,

    #[error("Other error: {0}")]
    Other(String),

    #[error("Failed to get capabilities")]
    Capabilities,

    #[error("Fast-path not supported: {0}")]
    FastPathNotSupported(String),

    #[error("DB Error: {0}")]
    Db(#[from] DbError),

    #[error("Tokio Postgres Error: {0}")]
    TokioPostgres(#[from] tokio_postgres::Error),
}

#[derive(Debug, Error)]
pub enum ProgressError {
    #[error("failed to load checkpoint: {0}")]
    LoadCheckpoint(String),
    #[error("failed to read wal entries: {0}")]
    Wal(String),
}

#[derive(Debug, Error)]
pub enum ConvertError {
    #[error("failed to convert AST to execution plan: {0}")]
    Plan(String),

    #[error("expression evaluation error: {0}")]
    Expression(String),

    #[error("connection error: {0}")]
    Connection(String),
}
