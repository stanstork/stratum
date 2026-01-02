use serde::Serialize;

#[derive(Serialize, Debug, Clone)]
pub struct RetryConfig {
    pub max_attempts: usize,
    pub backoff: BackoffConfig,
}

#[derive(Serialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BackoffConfig {
    /// Fixed delay between retries
    Fixed { delay: String },

    /// Exponential backoff (delay doubles each retry)
    Exponential {
        initial_delay: String,
        max_delay: Option<String>,
    },

    /// Linear backoff (delay increases by fixed amount)
    Linear { delay: String },
}
