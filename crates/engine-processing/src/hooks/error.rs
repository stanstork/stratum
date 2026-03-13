use connectors::error::DriverError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum HookError {
    #[error("Failed to execute hook statement #{index}: {sql}\nError: {source}")]
    ExecutionFailed {
        index: usize,
        sql: String,
        #[source]
        source: DriverError,
    },

    #[error("Driver error: {0}")]
    DriverError(#[from] DriverError),
}
