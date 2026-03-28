use thiserror::Error;

#[derive(Error, Debug)]
pub enum VerifyError {
    #[error("State store error: {0}")]
    Store(#[from] engine_state::error::StateStoreError),

    #[error("Driver error: {0}")]
    Driver(#[from] connectors::error::DriverError),

    #[error("Initialization error: {0}")]
    InitializationError(String),

    #[error("Unsupported driver: {0}")]
    UnsupportedDriver(String),

    #[error("No integrity receipts found — run `apply --integrity` first")]
    NoReceipts,

    #[error("Verification failed: one or more tables have mismatched hashes")]
    Mismatch,
}
