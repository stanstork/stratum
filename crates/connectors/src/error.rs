use crate::{
    file::csv::error::FileError,
    sql::base::error::{ConnectorError, DbError},
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AdapterError {
    /// An unsupported data format was requested.
    #[error("Unsupported format: {0}")]
    UnsupportedFormat(String),

    /// Adapter not found for the specified format.
    #[error("Adapter not found for format: {0}")]
    AdapterNotFound(String),

    /// Failed to initialize a data connector/adapter.
    #[error("Connector error: {0}")]
    Connector(#[from] ConnectorError),

    /// File-related error.
    #[error("File error: {0}")]
    FileError(#[from] FileError),

    /// Generic adapter error.
    #[error("Adapter error: {0}")]
    Generic(String),

    /// Database-related error.
    #[error("Database error: {0}")]
    Database(#[from] DbError),

    /// Invalid metadata error.
    #[error("Invalid Metadata: {0}")]
    InvalidMetadata(String),

    /// Unsupported driver error.
    #[error("Unsupported driver: {0}")]
    UnsupportedDriver(String),

    /// Missing required property error.
    #[error("Missing required property: {0}")]
    MissingProperty(String),
}
