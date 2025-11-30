use engine_config::settings::validated::ValidatedSettings;
use std::{num::NonZeroUsize, time::Duration};

/// Configuration for producer behavior.
#[derive(Debug, Clone)]
pub struct ProducerConfig {
    /// Number of rows to fetch per batch
    pub batch_size: usize,

    /// Concurrent transform operations
    pub transform_concurrency: NonZeroUsize,

    /// How long to wait when idle before polling again
    pub idle_poll_interval: Duration,

    /// Number of rows to sample for validation
    pub sample_size: usize,

    /// Maximum retry attempts for transient failures
    pub max_retries: usize,

    /// Delay between retries
    pub retry_delay: Duration,
}

impl Default for ProducerConfig {
    fn default() -> Self {
        Self {
            batch_size: 1000,
            transform_concurrency: NonZeroUsize::new(8).unwrap(),
            idle_poll_interval: Duration::from_millis(500),
            sample_size: 10,
            max_retries: 3,
            retry_delay: Duration::from_secs(1),
        }
    }
}

impl ProducerConfig {
    /// Create config from SMQL settings
    pub fn from_settings(settings: &ValidatedSettings) -> Self {
        Self {
            batch_size: settings.batch_size,
            ..Default::default()
        }
    }

    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    pub fn with_sample_size(mut self, size: usize) -> Self {
        self.sample_size = size;
        self
    }

    pub fn with_concurrency(mut self, concurrency: NonZeroUsize) -> Self {
        self.transform_concurrency = concurrency;
        self
    }
}
