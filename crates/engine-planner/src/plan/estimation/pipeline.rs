use crate::plan::estimation::duration::DurationEstimate;
use serde::Serialize;

#[derive(Serialize, Debug, Clone, Default)]
pub struct PipelineEstimations {
    pub duration: DurationEstimate,

    /// Estimated throughput (rows processed per second)
    pub rows_per_second: u64,

    /// Total number of batches to process
    pub batches: u64,

    /// Peak memory usage in megabytes
    pub memory_mb: u64,
}
