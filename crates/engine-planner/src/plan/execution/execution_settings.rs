use serde::Serialize;

#[derive(Serialize, Debug, Clone, Default)]
pub struct ExecutionSettings {
    /// "parallel" or "sequential"
    pub strategy: ExecutionStrategy,

    /// Max concurrent pipelines (when strategy = parallel)
    pub max_concurrency: usize,

    /// What to do when a pipeline fails
    pub on_failure: FailureStrategy,
}

#[derive(Serialize, Debug, Clone, PartialEq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionStrategy {
    #[default]
    Sequential,
    Parallel,
}

#[derive(Serialize, Debug, Clone, PartialEq, Default)]
#[serde(rename_all = "snake_case")]
pub enum FailureStrategy {
    /// Stop all pipelines on first failure
    FailFast,

    /// Continue other pipelines, fail dependents only
    #[default]
    Continue,

    /// Continue everything, report failures at end
    BestEffort,
}
