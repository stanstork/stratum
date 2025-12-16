use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// Execution configuration for DAG orchestration
/// Compiled from the execution {} block
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionConfig {
    /// Execution strategy: sequential or parallel
    pub strategy: ExecutionStrategy,

    /// Maximum number of pipelines running simultaneously (only for parallel strategy)
    pub max_concurrency: Option<u32>,

    /// Behavior when a pipeline fails
    pub on_failure: FailureStrategy,

    /// Maximum time for a single pipeline execution (in seconds)
    pub pipeline_timeout: Option<u64>,

    /// Maximum time for the entire migration (in seconds)
    pub total_timeout: Option<u64>,
}

/// Strategy for executing pipelines in the DAG
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutionStrategy {
    /// Execute pipelines one at a time (predictable, slower)
    Sequential,

    /// Execute independent pipelines concurrently (faster, requires max_concurrency)
    Parallel,
}

/// Strategy for handling pipeline failures
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FailureStrategy {
    /// Stop all execution immediately when any pipeline fails (default)
    FailFast,

    /// Skip failed pipeline's dependents, continue with independent pipelines
    Continue,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            strategy: ExecutionStrategy::Sequential,
            max_concurrency: None,
            on_failure: FailureStrategy::FailFast,
            pipeline_timeout: None,
            total_timeout: None,
        }
    }
}

impl ExecutionStrategy {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Sequential => "sequential",
            Self::Parallel => "parallel",
        }
    }
}

impl FromStr for ExecutionStrategy {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "sequential" => Ok(Self::Sequential),
            "parallel" => Ok(Self::Parallel),
            _ => Err(()),
        }
    }
}

impl FailureStrategy {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::FailFast => "fail_fast",
            Self::Continue => "continue",
        }
    }
}

impl FromStr for FailureStrategy {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "fail_fast" | "failfast" => Ok(Self::FailFast),
            "continue" => Ok(Self::Continue),
            _ => Err(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_execution_config() {
        let config = ExecutionConfig::default();
        assert_eq!(config.strategy, ExecutionStrategy::Sequential);
        assert_eq!(config.on_failure, FailureStrategy::FailFast);
        assert!(config.max_concurrency.is_none());
        assert!(config.pipeline_timeout.is_none());
        assert!(config.total_timeout.is_none());
    }

    #[test]
    fn test_execution_strategy_from_str() {
        assert_eq!(
            ExecutionStrategy::from_str("sequential"),
            Ok(ExecutionStrategy::Sequential)
        );
        assert_eq!(
            ExecutionStrategy::from_str("parallel"),
            Ok(ExecutionStrategy::Parallel)
        );
        assert_eq!(
            ExecutionStrategy::from_str("PARALLEL"),
            Ok(ExecutionStrategy::Parallel)
        );
        assert_eq!(ExecutionStrategy::from_str("invalid"), Err(()));
    }

    #[test]
    fn test_failure_strategy_from_str() {
        assert_eq!(
            FailureStrategy::from_str("fail_fast"),
            Ok(FailureStrategy::FailFast)
        );
        assert_eq!(
            FailureStrategy::from_str("failfast"),
            Ok(FailureStrategy::FailFast)
        );
        assert_eq!(
            FailureStrategy::from_str("continue"),
            Ok(FailureStrategy::Continue)
        );
        assert_eq!(
            FailureStrategy::from_str("CONTINUE"),
            Ok(FailureStrategy::Continue)
        );
        assert_eq!(FailureStrategy::from_str("invalid"), Err(()));
    }

    #[test]
    fn test_strategy_round_trip() {
        let seq = ExecutionStrategy::Sequential;
        assert_eq!(ExecutionStrategy::from_str(seq.as_str()), Ok(seq));

        let par = ExecutionStrategy::Parallel;
        assert_eq!(ExecutionStrategy::from_str(par.as_str()), Ok(par));
    }
}
