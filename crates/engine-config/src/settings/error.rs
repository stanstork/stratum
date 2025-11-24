use connectors::{error::AdapterError, sql::base::error::DbError};
use thiserror::Error;

/// Errors raised when processing migration settings or configuration.
#[derive(Debug, Error)]
pub enum SettingsError {
    /// A required source column was missing from the schema.
    #[error("Missing source column: {0}")]
    MissingSourceColumn(String),

    /// The chosen destination type isn't supported.
    #[error("Unsupported destination: {0}")]
    UnsupportedDestination(String),

    /// The chosen source type isn't supported.
    #[error("Unsupported source: {0}")]
    UnsupportedSource(String),

    /// The chosen destination format (e.g. FILE vs TABLE) isn't supported.
    #[error("Unsupported destination format: {0}")]
    UnsupportedDestinationFormat(String),

    /// A database error occurred while validating or applying settings.
    #[error("Database error in settings: {0}")]
    Database(#[from] DbError),

    /// We failed to infer a data type for one of the fields.
    #[error("Data type inference error: {0}")]
    DataTypeInference(String),

    /// Adapter-related error.
    #[error("Adapter error: {0}")]
    AdapterError(#[from] AdapterError),
}
