use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// Status of a pipeline in the migration lifecycle
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum PipelineStatus {
    /// Waiting for dependencies to complete
    #[default]
    Pending,
    /// Dependencies met, assigned to an execution slot
    Queued,
    /// Actively processing data
    Running,
    /// Execution suspended by user or system
    Paused,
    /// Successfully finished processing all data
    Completed,
    /// Failed with a specific error message
    Failed(String),
    /// Skipped because a dependency failed
    Skipped,
}

impl PipelineStatus {
    /// Returns true if the pipeline has reached a final, non-active state
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            PipelineStatus::Completed | PipelineStatus::Failed(_) | PipelineStatus::Skipped
        )
    }
}

/// Tracks data throughput using a sliding window for smoother estimations
#[derive(Debug, Clone)]
pub struct ThroughputTracker {
    /// Historical samples: (Instant of recording, Total rows processed at that time)
    samples: VecDeque<(Instant, u64)>,
    /// The temporal length of the sliding window
    window: Duration,
    /// Peak throughput observed (rows/sec)
    peak_throughput: f64,
    /// Cached current throughput value
    cached_current: f64,
}

impl ThroughputTracker {
    /// Creates a new throughput tracker with specified window duration
    pub fn new(window: Duration) -> Self {
        Self {
            samples: VecDeque::with_capacity(128),
            window,
            peak_throughput: 0.0,
            cached_current: 0.0,
        }
    }

    /// Records current progress and purges samples outside the window
    pub fn record(&mut self, total_rows: u64) {
        let now = Instant::now();
        self.samples.push_back((now, total_rows));

        // Retain only samples within the window
        self.prune_old_samples(now);

        // Recalculate and cache current throughput
        self.recalculate_throughput();
    }

    /// Removes samples that fall outside the current window
    fn prune_old_samples(&mut self, now: Instant) {
        while self.samples.len() > 2 {
            if let Some((timestamp, _)) = self.samples.front()
                && now.duration_since(*timestamp) > self.window
            {
                self.samples.pop_front();
                continue;
            }
            break;
        }
    }

    /// Recalculates current throughput and updates peak
    fn recalculate_throughput(&mut self) {
        if self.samples.len() < 2 {
            self.cached_current = 0.0;
            return;
        }

        let (start_time, start_rows) = self.samples.front().unwrap();
        let (end_time, end_rows) = self.samples.back().unwrap();

        let duration = end_time.duration_since(*start_time).as_secs_f64();
        if duration <= 0.001 {
            // Prevent division by near-zero
            self.cached_current = 0.0;
            return;
        }

        let throughput = (end_rows.saturating_sub(*start_rows) as f64) / duration;
        self.cached_current = throughput;

        // Track peak throughput
        if throughput > self.peak_throughput {
            self.peak_throughput = throughput;
        }
    }

    /// Returns the current throughput (rows/sec) within the sliding window
    pub fn current_throughput(&self) -> f64 {
        self.cached_current
    }

    /// Returns the peak throughput observed (rows/sec)
    pub fn peak_throughput(&self) -> f64 {
        self.peak_throughput
    }

    /// Estimates the time remaining based on current throughput
    pub fn eta(&self, remaining_rows: u64) -> Option<Duration> {
        let rate = self.current_throughput();
        if rate <= 0.1 {
            // Too slow to estimate reliably
            return None;
        }

        let seconds = (remaining_rows as f64) / rate;
        Some(Duration::from_secs_f64(seconds))
    }
}

/// Represents the real-time state of an individual migration pipeline
#[derive(Debug, Clone)]
pub struct PipelineState {
    pub name: String,
    pub status: PipelineStatus,

    // Counters
    pub source_rows: u64,
    pub processed_rows: u64,
    pub failed_rows: u64,
    pub skipped_rows: u64,
    pub bytes_transferred: u64,

    // Batching info
    pub current_batch: u32,
    pub total_batches: u32,

    // Metrics & Timing
    pub throughput: ThroughputTracker,
    pub started_at: Option<Instant>,
    pub completed_at: Option<Instant>,
    pub last_error: Option<String>,

    /// The stage index in the DAG execution plan
    pub stage: u32,
}

impl PipelineState {
    /// Creates a new pipeline state
    pub fn new(name: String, stage: u32) -> Self {
        Self {
            name,
            status: PipelineStatus::Pending,
            source_rows: 0,
            processed_rows: 0,
            failed_rows: 0,
            skipped_rows: 0,
            bytes_transferred: 0,
            current_batch: 0,
            total_batches: 0,
            throughput: ThroughputTracker::new(Duration::from_secs(30)),
            started_at: None,
            completed_at: None,
            last_error: None,
            stage,
        }
    }

    /// Returns progress as a percentage (0-100)
    pub fn progress(&self) -> f64 {
        // If pipeline is completed, always show 100% even if some rows were skipped/failed
        if self.status == PipelineStatus::Completed {
            return 100.0;
        }

        if self.source_rows == 0 {
            0.0
        } else {
            ((self.processed_rows as f64 / self.source_rows as f64) * 100.0).clamp(0.0, 100.0)
        }
    }

    /// Returns progress as a fraction between 0.0 and 1.0
    pub fn progress_fraction(&self) -> f64 {
        self.progress() / 100.0
    }

    /// Returns the total time spent processing
    /// If finished, returns total duration; if running, returns elapsed time
    pub fn duration(&self) -> Option<Duration> {
        match (self.started_at, self.completed_at) {
            (Some(start), Some(end)) => Some(end.duration_since(start)),
            (Some(start), None) => Some(start.elapsed()),
            _ => None,
        }
    }

    /// Returns the estimated time remaining based on current throughput
    pub fn eta(&self) -> Option<Duration> {
        if self.status.is_terminal() || self.processed_rows >= self.source_rows {
            return Some(Duration::ZERO);
        }
        let remaining = self.source_rows.saturating_sub(self.processed_rows);
        self.throughput.eta(remaining)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pipeline_state_new() {
        let state = PipelineState::new("test_pipeline".to_string(), 1);
        assert_eq!(state.name, "test_pipeline");
        assert_eq!(state.stage, 1);
        assert_eq!(state.status, PipelineStatus::Pending);
        assert_eq!(state.progress(), 0.0);
    }

    #[test]
    fn test_progress_percentage() {
        let mut state = PipelineState::new("test".to_string(), 0);
        state.source_rows = 100;
        state.processed_rows = 50;
        assert_eq!(state.progress(), 50.0);
        assert_eq!(state.progress_fraction(), 0.5);
    }

    #[test]
    fn test_progress_completed() {
        let mut state = PipelineState::new("test".to_string(), 0);
        state.source_rows = 0;
        state.status = PipelineStatus::Completed;
        assert_eq!(state.progress(), 100.0);
    }

    #[test]
    fn test_progress_completed_with_skipped_rows() {
        // Test that completed pipelines show 100% even if some rows were skipped
        let mut state = PipelineState::new("test".to_string(), 0);
        state.source_rows = 100;
        state.processed_rows = 95;
        state.skipped_rows = 5;
        state.status = PipelineStatus::Completed;

        // Should show 100% progress since pipeline is completed
        assert_eq!(state.progress(), 100.0);
        assert_eq!(state.progress_fraction(), 1.0);
    }

    #[test]
    fn test_pipeline_status_terminal() {
        assert!(PipelineStatus::Completed.is_terminal());
        assert!(PipelineStatus::Failed("error".to_string()).is_terminal());
        assert!(PipelineStatus::Skipped.is_terminal());
        assert!(!PipelineStatus::Running.is_terminal());
    }

    #[test]
    fn test_throughput_tracker() {
        let mut tracker = ThroughputTracker::new(Duration::from_secs(60));
        tracker.record(100);
        std::thread::sleep(Duration::from_millis(100));
        tracker.record(200);

        // Should have some throughput
        assert!(tracker.current_throughput() > 0.0);
    }

    #[test]
    fn test_throughput_eta() {
        let mut tracker = ThroughputTracker::new(Duration::from_secs(60));
        tracker.record(100);
        std::thread::sleep(Duration::from_millis(100));
        tracker.record(200);

        let eta = tracker.eta(100);
        assert!(eta.is_some());
    }
}
