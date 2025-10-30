use crate::{file::csv::error::FileError, sql::base::error::ConnectorError};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AdapterError {
    /// An unsupported data format was requested.
    #[error("Unsupported format: {0}")]
    UnsupportedFormat(String),

    /// Failed to initialize a data connector/adapter.
    #[error("Connector error: {0}")]
    Connector(#[from] ConnectorError),

    /// Failed to initialize a file adapter.
    #[error("File error: {0}")]
    FileError(#[from] FileError),
}
