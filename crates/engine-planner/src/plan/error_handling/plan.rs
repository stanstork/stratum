use crate::plan::error_handling::{failed_rows::FailedRowsConfig, retry::RetryConfig};
use serde::Serialize;

#[derive(Serialize, Debug, Clone, Default)]
pub struct ErrorHandlingPlan {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry: Option<RetryConfig>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub failed_rows: Option<FailedRowsConfig>,

    /// What to do after all retries are exhausted
    pub after_max_retries: AfterMaxRetries,
}

#[derive(Serialize, Debug, Clone, PartialEq, Default)]
#[serde(rename_all = "snake_case")]
pub enum AfterMaxRetries {
    #[default]
    Fail,
    Continue,
    SkipBatch,
}
