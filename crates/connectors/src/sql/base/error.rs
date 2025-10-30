use std::string::FromUtf8Error;
use thiserror::Error;

/// All errors coming from the database/query layer.
#[derive(Debug, Error)]
pub enum DbError {
    /// Low‐level I/O failure.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Any SQL driver error.
    #[error("SQL error: {0}")]
    Sql(#[from] sqlx::Error),

    /// We detected a circular reference in the metadata graph.
    #[error("Circular reference detected: {0}")]
    CircularReference(String),

    /// UTF-8 decoding failed on some byte data.
    #[error("UTF-8 conversion error: {0}")]
    Utf8(#[from] FromUtf8Error),

    /// Writing rows to the database failed at the application level.
    #[error("Write error: {0}")]
    Write(String),

    /// A mis-configured or unsupported database adapter was specified.
    #[error("Invalid adapter: {0}")]
    InvalidAdapter(String),

    /// An error occurred while building a SQL query.
    #[error("Query build error: {0}")]
    QueryBuildError(String),

    #[error("Unknown error: {0}")]
    Unknown(String),
}

/// Errors happening during adapter or connection setup.
#[derive(Debug, Error)]
pub enum ConnectorError {
    /// SQLx failed to build the connection or pool.
    #[error("SQLx connector creation failed: {0}")]
    Sqlx(#[from] sqlx::Error),
}
