use crate::plan::estimation::duration::DurationEstimate;
use serde::Serialize;

/// Execution stages for DAG-based scheduling.
/// Pipelines within the same stage can execute in parallel (if parallel strategy is enabled).
/// Stages execute sequentially in order.
#[derive(Serialize, Debug, Clone)]
pub struct ExecutionStage {
    /// Stage number (0-indexed)
    pub stage: usize,
    /// Pipeline names that can run in this stage (parallel execution possible)
    pub pipelines: Vec<String>,
    /// Total estimated duration for all pipelines in this stage
    pub estimated_duration: DurationEstimate,
}
