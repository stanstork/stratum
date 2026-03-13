use std::string::FromUtf8Error;
use thiserror::Error;

/// All errors coming from the database/query layer.
#[derive(Debug, Error)]
pub enum DbError {
    /// Low‐level I/O failure.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Any MySQL driver error.
    #[error("MySQL error: {0}")]
    MySqlError(#[from] mysql_async::Error),

    /// Any Pg driver error.
    #[error("Pg error: {0}")]
    PgError(#[from] tokio_postgres::Error),

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

    /// An unknown error occurred.
    #[error("Unknown error: {0}")]
    Unknown(String),
}

#[derive(Debug, Error)]
pub enum DriverError {
    #[error("Unsupported scheme: {0}")]
    UnsupportedScheme(String),

    #[error("Unsupported format: {0}")]
    UnsupportedFormat(String),

    #[error("Driver not found for driver id: {0}")]
    DriverNotFound(String),

    #[error("Invalid URL: {0}")]
    InvalidUrl(String),

    #[error("Database error: {0}")]
    DatabaseError(#[from] DbError),

    #[error("Circular reference detected: {0}")]
    CircularReference(String),

    #[error("Connection error: {0}")]
    ConnectionError(String),

    #[error("Query error: {0}")]
    QueryError(String),

    #[error("Transaction error: {0}")]
    TransactionError(String),

    /// Any MySQL driver error.
    #[error("MySQL error: {0}")]
    MySqlError(#[from] mysql_async::Error),

    /// Any Pg driver error.
    #[error("Pg error: {0}")]
    PgError(#[from] tokio_postgres::Error),

    #[error("Unknown error: {0}")]
    Unknown(String),

    #[error("Unsupported driver: {0}")]
    UnsupportedDriver(String),
}
