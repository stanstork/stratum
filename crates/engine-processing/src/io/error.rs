use connectors::error::{DbError, DriverError};
use thiserror::Error;

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

    #[error("Driver Error: {0}")]
    Driver(#[from] DriverError),

    #[error("Tokio Postgres Error: {0}")]
    TokioPostgres(#[from] tokio_postgres::Error),
}
