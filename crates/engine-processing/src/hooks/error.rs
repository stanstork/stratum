use connectors::error::AdapterError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum HookError {
    #[error("Failed to execute hook statement #{index}: {sql}\nError: {source}")]
    ExecutionFailed {
        index: usize,
        sql: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("Adapter error: {0}")]
    AdapterError(#[from] AdapterError),
}
