use thiserror::Error;

/// Errors happening during adapter or connection setup.
#[derive(Debug, Error)]
pub enum ConnectorError {
    /// SQLx failed to build the connection or pool.
    #[error("SQLx connector creation failed: {0}")]
    Sqlx(#[from] sqlx::Error),
}
