use std::time::{Duration, Instant};

/// Default cadence for progress reporting on a long scan.
pub const PROGRESS_INTERVAL: Duration = Duration::from_secs(1);

/// Time-throttled gate for progress reporting inside a long, quiet loop.
pub struct Ticker {
    last: Instant,
    every: Duration,
    /// Items between clock reads; 1 means every call.
    sample: u64,
    /// Which sampling window the last call fell in.
    window: u64,
}

impl Ticker {
    /// Report no more than once per `every`, checking the clock on every call.
    pub fn new(every: Duration) -> Self {
        Self {
            last: Instant::now(),
            every,
            sample: 1,
            window: 0,
        }
    }

    /// Consult the clock only once every `sample` items.
    pub fn sampling(mut self, sample: u64) -> Self {
        self.sample = sample.max(1);
        self
    }

    /// True when enough time has passed to report again.
    pub fn report(&mut self, done: u64) -> bool {
        let window = done / self.sample;
        if window == self.window {
            return false;
        }
        self.window = window;

        if self.last.elapsed() < self.every {
            return false;
        }

        self.last = Instant::now();
        true
    }
}
