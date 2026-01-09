use crate::tui::pipeline::{PipelineState, PipelineStatus};
use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

/// Global statistics tracking for the migration
#[derive(Debug, Clone)]
pub struct GlobalStats {
    pub total_pipelines: usize,
    pub completed_pipelines: usize,
    pub failed_pipelines: usize,
    pub running_pipelines: usize,

    pub total_source_rows: u64,
    pub total_processed_rows: u64,
    pub total_failed_rows: u64,
    pub total_skipped_rows: u64,
    pub total_bytes_transferred: u64,

    pub throughput_history: VecDeque<(Instant, f64)>,
    pub current_throughput: f64,
    pub peak_throughput: f64,
    pub average_throughput: f64,
    pub current_bytes_per_second: f64,
    pub peak_bytes_per_second: f64,

    pub started_at: Instant,
    pub estimated_completion: Option<Instant>,

    pub total_batches: u64,
    pub completed_batches: u64,
}

impl Default for GlobalStats {
    fn default() -> Self {
        Self {
            total_pipelines: 0,
            completed_pipelines: 0,
            failed_pipelines: 0,
            running_pipelines: 0,
            total_source_rows: 0,
            total_processed_rows: 0,
            total_failed_rows: 0,
            total_skipped_rows: 0,
            total_bytes_transferred: 0,
            throughput_history: VecDeque::with_capacity(60), // Keep last 60 samples
            current_throughput: 0.0,
            peak_throughput: 0.0,
            average_throughput: 0.0,
            current_bytes_per_second: 0.0,
            peak_bytes_per_second: 0.0,
            started_at: Instant::now(),
            estimated_completion: None,
            total_batches: 0,
            completed_batches: 0,
        }
    }
}

impl GlobalStats {
    /// Synchronize global stats from pipeline states
    pub fn sync_from_pipelines<'a, I>(&mut self, pipelines: I)
    where
        I: Iterator<Item = &'a PipelineState>,
    {
        let now = Instant::now();

        // Reset counters
        self.reset_counters();

        // Aggregate from all pipelines
        for pipeline in pipelines {
            self.aggregate_pipeline_stats(pipeline);
        }

        // Calculate derived metrics
        self.update_throughput_metrics(now);
        self.update_bytes_per_second(now);
        self.record_throughput_history(now);
        self.calculate_average_throughput();
        self.estimate_completion(now);
    }

    fn reset_counters(&mut self) {
        self.completed_pipelines = 0;
        self.failed_pipelines = 0;
        self.running_pipelines = 0;
        self.total_source_rows = 0;
        self.total_processed_rows = 0;
        self.total_failed_rows = 0;
        self.total_bytes_transferred = 0;
        self.total_batches = 0;
        self.completed_batches = 0;
    }

    fn aggregate_pipeline_stats(&mut self, pipeline: &PipelineState) {
        self.total_source_rows += pipeline.source_rows;
        self.total_processed_rows += pipeline.processed_rows;
        self.total_failed_rows += pipeline.failed_rows;
        self.total_bytes_transferred += pipeline.bytes_transferred;
        self.total_batches += pipeline.total_batches as u64;
        self.completed_batches += pipeline.current_batch as u64;

        match pipeline.status {
            PipelineStatus::Completed => self.completed_pipelines += 1,
            PipelineStatus::Running => self.running_pipelines += 1,
            PipelineStatus::Failed(_) => self.failed_pipelines += 1,
            _ => {}
        }
    }

    fn update_throughput_metrics(&mut self, _now: Instant) {
        // Peak throughput is now updated in calculate_current_throughput()
        // This method is kept for potential future throughput metrics
    }

    fn update_bytes_per_second(&mut self, now: Instant) {
        let elapsed = now.duration_since(self.started_at).as_secs_f64();
        if elapsed > 0.0 && self.total_bytes_transferred > 0 {
            self.current_bytes_per_second = self.total_bytes_transferred as f64 / elapsed;
            if self.current_bytes_per_second > self.peak_bytes_per_second {
                self.peak_bytes_per_second = self.current_bytes_per_second;
            }
        }
    }

    fn record_throughput_history(&mut self, now: Instant) {
        let last_time = self.throughput_history.back().map(|(t, _)| *t);
        let should_record =
            last_time.is_none_or(|t| now.duration_since(t) >= Duration::from_secs(1));

        if should_record {
            self.throughput_history
                .push_back((now, self.current_throughput));
            if self.throughput_history.len() > 60 {
                self.throughput_history.pop_front();
            }
        }
    }

    fn calculate_average_throughput(&mut self) {
        if !self.throughput_history.is_empty() {
            self.average_throughput = self
                .throughput_history
                .iter()
                .map(|(_, val)| val)
                .sum::<f64>()
                / self.throughput_history.len() as f64;
        }
    }

    fn estimate_completion(&mut self, now: Instant) {
        if self.total_source_rows > self.total_processed_rows && self.current_throughput > 0.0 {
            let remaining = self.total_source_rows - self.total_processed_rows;
            let seconds_remaining = remaining as f64 / self.current_throughput;
            self.estimated_completion = Some(now + Duration::from_secs_f64(seconds_remaining));
        } else {
            self.estimated_completion = None;
        }
    }

    /// Calculate current and peak throughput from all pipelines
    pub fn calculate_current_throughput<'a, I>(&mut self, pipelines: I)
    where
        I: Iterator<Item = &'a PipelineState>,
    {
        let pipelines: Vec<_> = pipelines.collect();

        // Sum current throughput from running pipelines
        self.current_throughput = pipelines
            .iter()
            .filter(|p| p.status == PipelineStatus::Running)
            .map(|p| p.throughput.current_throughput())
            .sum();

        // Peak throughput is the sum of individual pipeline peaks
        // This ensures we capture the maximum rate even if pipelines don't peak simultaneously
        self.peak_throughput = pipelines
            .iter()
            .map(|p| p.throughput.peak_throughput())
            .sum();
    }
}
