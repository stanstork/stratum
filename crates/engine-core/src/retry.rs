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
