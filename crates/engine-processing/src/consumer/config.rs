use std::time::Duration;

/// Configuration for consumer behavior.
#[derive(Clone, Debug)]
pub struct ConsumerConfig {
    /// Maximum number of batches to buffer before applying backpressure
    pub max_pending_batches: usize,

    /// Timeout for batch processing operations
    pub batch_timeout: Duration,

    /// Number of retries for transient write failures
    pub max_retries: usize,

    /// Delay between retry attempts
    pub retry_delay: Duration,

    /// Flush interval for periodic commits
    pub flush_interval: Duration,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            max_pending_batches: 10,
            batch_timeout: Duration::from_secs(30),
            max_retries: 3,
            retry_delay: Duration::from_secs(1),
            flush_interval: Duration::from_secs(5),
        }
    }
}

impl ConsumerConfig {
    pub fn with_max_pending(mut self, max: usize) -> Self {
        self.max_pending_batches = max;
        self
    }
}
