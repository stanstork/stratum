use std::time::Duration;

#[derive(Clone, Debug)]
pub struct CircuitBreaker {
    threshold: u32,
    consecutive_failures: u32,
    base_delay: Duration,
    max_delay: Duration,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum CircuitBreakerState {
    RetryAfter(Duration),
    Open,
}

impl CircuitBreaker {
    pub fn new(threshold: u32, base_delay: Duration, max_delay: Duration) -> Self {
        Self {
            threshold: threshold.max(1),
            consecutive_failures: 0,
            base_delay,
            max_delay: if max_delay.is_zero() {
                base_delay
            } else {
                max_delay
            },
        }
    }

    pub fn default_db() -> Self {
        Self::new(4, Duration::from_secs(1), Duration::from_secs(30))
    }

    pub fn record_failure(&mut self) -> CircuitBreakerState {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        if self.consecutive_failures >= self.threshold {
            CircuitBreakerState::Open
        } else {
            let delay = self.delay_for(self.consecutive_failures);
            CircuitBreakerState::RetryAfter(delay)
        }
    }

    pub fn record_success(&mut self) {
        self.consecutive_failures = 0;
    }

    pub fn consecutive_failures(&self) -> u32 {
        self.consecutive_failures
    }

    fn delay_for(&self, failures: u32) -> Duration {
        if self.base_delay.is_zero() {
            return Duration::from_millis(0);
        }

        let exponent = failures.saturating_sub(1).min(6);
        let factor = 1u128 << exponent;
        let base_ms = self.base_delay.as_millis();
        let delay_ms = base_ms.saturating_mul(factor);
        let capped = delay_ms.min(self.max_delay.as_millis());
        Duration::from_millis(capped as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transient_failure_then_success_resets_breaker() {
        let mut breaker = CircuitBreaker::new(3, Duration::from_millis(1), Duration::from_secs(1));

        match breaker.record_failure() {
            CircuitBreakerState::RetryAfter(delay) => assert!(delay > Duration::from_millis(0)),
            CircuitBreakerState::Open => panic!("breaker should not open on first failure"),
        }

        breaker.record_success();

        match breaker.record_failure() {
            CircuitBreakerState::RetryAfter(_) => {}
            CircuitBreakerState::Open => panic!("breaker should reset after success"),
        }
    }

    #[test]
    fn permanent_failure_opens_breaker_and_stays_open() {
        let mut breaker = CircuitBreaker::new(2, Duration::from_millis(1), Duration::from_secs(1));
        assert!(matches!(
            breaker.record_failure(),
            CircuitBreakerState::RetryAfter(_)
        ));

        let state = breaker.record_failure();
        assert!(matches!(state, CircuitBreakerState::Open));
        assert_eq!(breaker.consecutive_failures(), 2);

        let next_state = breaker.record_failure();
        assert!(matches!(next_state, CircuitBreakerState::Open));
        assert_eq!(breaker.consecutive_failures(), 3);
    }
}
