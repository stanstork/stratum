use thiserror::Error;

/// Top‐level errors for the data migration engine.
#[derive(Debug, Error)]
pub enum ContextError {
    /// Error occurred while retrieving the adapter from the context for the database source.
    #[error("Adapter not found: {0}")]
    AdapterNotFound(String),
}
