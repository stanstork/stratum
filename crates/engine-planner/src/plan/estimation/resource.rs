use crate::plan::estimation::duration::DurationEstimate;
use serde::{Serialize, Serializer};

fn round_f64<S>(value: &f64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_f64((*value * 1000.0).round() / 1000.0)
}

#[derive(Serialize, Debug, Clone, Default)]
pub struct ResourceEstimations {
    pub duration: DurationEstimate,

    /// Peak memory usage across all pipelines (megabytes)
    pub peak_memory_mb: u64,

    /// Total network transfer between source and destination (megabytes)
    #[serde(serialize_with = "round_f64")]
    pub network_transfer_mb: f64,

    /// Disk space needed for temporary files and checkpoints (megabytes)
    pub disk_usage_mb: u64,

    /// Total batches across all pipelines
    pub total_batches: u64,
}
