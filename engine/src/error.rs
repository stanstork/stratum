use crate::settings::error::SettingsError;
use sql_adapter::error::{adapter::ConnectorError, db::DbError};
use thiserror::Error;

/// Top‐level errors for the data migration engine.
#[derive(Debug, Error)]
pub enum MigrationError {
    /// Failed to initialize a data connector/adapter.
    #[error("Connector error: {0}")]
    Connector(#[from] ConnectorError),

    // Any error coming from the database layer.
    #[error("Database error: {0}")]
    Database(#[from] DbError),

    /// An unsupported data format was requested.
    #[error("Unsupported format: {0}")]
    UnsupportedFormat(String),

    /// Something went wrong applying migration settings.
    #[error("Settings application error: {0}")]
    Settings(#[from] SettingsError),
}
