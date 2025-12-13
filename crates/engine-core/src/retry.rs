use model::execution::pipeline::{BackoffStrategy, RetryConfig};
use std::future::Future;
use std::time::Duration;
use tokio::time::sleep;

/// Indicates whether an error should be retried or treated as fatal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryDisposition {
    Retry,
    Stop,
}

/// Result of running an operation under the retry policy.
#[derive(Debug)]
pub enum RetryError<E> {
    /// The error was considered fatal and should bubble up immediately.
    Fatal(E),
    /// The error was retryable, but the configured attempts were exhausted.
    AttemptsExceeded(E),
}

#[derive(Debug, Clone)]
pub struct RetryPolicy {
    pub max_attempts: usize,
    pub base_delay: Duration,
    pub max_delay: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            base_delay: Duration::from_millis(200),
            max_delay: Duration::from_secs(5),
        }
    }
}

impl RetryPolicy {
    pub fn new(max_attempts: usize, base_delay: Duration, max_delay: Duration) -> Self {
        Self {
            max_attempts: max_attempts.max(1),
            base_delay,
            max_delay: if max_delay.is_zero() {
                base_delay
            } else {
                max_delay
            },
        }
    }

    /// Preset tuned for database/network calls (higher delay, more attempts).
    pub fn for_database() -> Self {
        Self {
            max_attempts: 5,
            base_delay: Duration::from_millis(250),
            max_delay: Duration::from_secs(5),
        }
    }

    /// Create a RetryPolicy from a RetryConfig, with optional fallback to database defaults.
    pub fn from_config(config: Option<&RetryConfig>) -> Self {
        match config {
            Some(cfg) => {
                let base_delay = Duration::from_millis(cfg.delay_ms);
                // Calculate max_delay based on backoff strategy
                let max_delay = match cfg.backoff {
                    BackoffStrategy::Fixed => base_delay,
                    BackoffStrategy::Linear => {
                        // Linear: max_delay = base * max_attempts
                        base_delay.saturating_mul(cfg.max_attempts)
                    }
                    BackoffStrategy::Exponential => {
                        // Exponential: cap at 5 seconds for safety
                        Duration::from_secs(5)
                    }
                };

                Self::new(cfg.max_attempts as usize, base_delay, max_delay)
            }
            None => Self::for_database(),
        }
    }

    /// Executes the operation with the configured retry policy.
    pub async fn run<F, Fut, T, E, Classifier>(
        &self,
        mut op: F,
        classify: Classifier,
    ) -> Result<T, RetryError<E>>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, E>>,
        Classifier: Fn(&E) -> RetryDisposition,
    {
        let mut attempt = 0;

        loop {
            match op().await {
                Ok(result) => return Ok(result),
                Err(err) => match classify(&err) {
                    RetryDisposition::Stop => return Err(RetryError::Fatal(err)),
                    RetryDisposition::Retry => {
                        if attempt + 1 >= self.max_attempts {
                            return Err(RetryError::AttemptsExceeded(err));
                        }

                        let delay = self.backoff_delay(attempt);
                        sleep(delay).await;
                        attempt += 1;
                    }
                },
            }
        }
    }

    fn backoff_delay(&self, attempt: usize) -> Duration {
        if self.base_delay.is_zero() {
            return Duration::from_millis(0);
        }

        let factor = 1u128 << attempt.min(6);
        let base_ms = self.base_delay.as_millis();
        let delay_ms = base_ms.saturating_mul(factor);
        let capped = delay_ms.min(self.max_delay.as_millis());
        Duration::from_millis(capped as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    #[derive(Debug, Clone)]
    struct TestError(&'static str);

    #[tokio::test]
    async fn retries_transient_failure_and_succeeds() {
        let policy = RetryPolicy::new(5, Duration::from_millis(0), Duration::from_millis(0));
        let attempts = Arc::new(AtomicUsize::new(0));
        let op_attempts = attempts.clone();

        let result = policy
            .run(
                move || {
                    let op_attempts = op_attempts.clone();
                    async move {
                        let attempt = op_attempts.fetch_add(1, Ordering::SeqCst);
                        if attempt < 2 {
                            Err(TestError("transient"))
                        } else {
                            Ok::<&'static str, TestError>("done")
                        }
                    }
                },
                |err: &TestError| match err.0 {
                    "transient" => RetryDisposition::Retry,
                    _ => RetryDisposition::Stop,
                },
            )
            .await;

        assert!(result.is_ok());
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn permanent_failure_exhausts_retries() {
        let policy = RetryPolicy::new(3, Duration::from_millis(0), Duration::from_millis(0));
        let attempts = Arc::new(AtomicUsize::new(0));
        let op_attempts = attempts.clone();

        let result = policy
            .run(
                move || {
                    let op_attempts = op_attempts.clone();
                    async move {
                        op_attempts.fetch_add(1, Ordering::SeqCst);
                        Err::<(), TestError>(TestError("permanent"))
                    }
                },
                |_err: &TestError| RetryDisposition::Retry,
            )
            .await;

        match result {
            Err(RetryError::AttemptsExceeded(TestError(msg))) => {
                assert_eq!(msg, "permanent");
            }
            other => panic!("unexpected result: {other:?}"),
        }
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }
}
