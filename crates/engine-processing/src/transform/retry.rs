use crate::transform::error::{ErrorType, TransformError};
use engine_core::retry::{RetryDisposition, RetryError, RetryPolicy};
use model::{
    core::value::Value,
    execution::{
        failed_row::{FailedRow, ProcessingStage},
        pipeline::{
            BackoffStrategy, ErrorHandling, FailedRowsAction, FailedRowsConfig, RetryConfig,
        },
    },
    records::row::RowData,
};
use std::{collections::HashMap, time::Duration};
use tracing::{debug, warn};

/// Executor that handles transform retries and failed row tracking
pub struct TransformRetryExecutor {
    pipeline_name: String,
    error_handling: Option<ErrorHandling>,
    retry_policy: RetryPolicy,
}

impl TransformRetryExecutor {
    pub fn new(pipeline_name: String, error_handling: Option<ErrorHandling>) -> Self {
        let retry_policy = if let Some(ref eh) = error_handling
            && let Some(ref retry_config) = eh.retry
        {
            Self::build_retry_policy(retry_config)
        } else {
            RetryPolicy::default()
        };

        Self {
            pipeline_name,
            error_handling,
            retry_policy,
        }
    }

    /// Execute a transform operation with retry logic
    pub async fn execute<F, Fut>(
        &self,
        row: &mut RowData,
        mut operation: F,
    ) -> Result<(), FailedRow>
    where
        F: FnMut(&mut RowData) -> Fut,
        Fut: std::future::Future<Output = Result<(), TransformError>>,
    {
        let result = self
            .retry_policy
            .run(
                || {
                    let row = unsafe { &mut *(row as *mut RowData) };
                    operation(row)
                },
                |err: &TransformError| self.classify_transform_error(err),
            )
            .await;

        match result {
            Ok(_) => Ok(()),
            Err(RetryError::Fatal(err)) => {
                debug!(
                    "Transform failed with fatal error for pipeline '{}': {}",
                    self.pipeline_name, err
                );
                Err(self.create_failed_row(row, err, 1))
            }
            Err(RetryError::AttemptsExceeded(err)) => {
                warn!(
                    "Transform retry attempts exhausted for pipeline '{}': {}",
                    self.pipeline_name, err
                );
                let max_attempts = self
                    .error_handling
                    .as_ref()
                    .and_then(|eh| eh.retry.as_ref())
                    .map(|r| r.max_attempts)
                    .unwrap_or(3);
                Err(self.create_failed_row(row, err, max_attempts))
            }
        }
    }

    fn classify_transform_error(&self, err: &TransformError) -> RetryDisposition {
        match err.error_type() {
            ErrorType::Transient => RetryDisposition::Retry,
            ErrorType::Permanent => RetryDisposition::Stop,
        }
    }

    fn create_failed_row(
        &self,
        row: &RowData,
        error: TransformError,
        attempt_number: u32,
    ) -> FailedRow {
        let original_data = self.row_to_hashmap(row);
        let error_type = format!("{:?}", error.error_type());
        let error_message = error.to_string();

        let mut failed_row = FailedRow::new(
            self.pipeline_name.clone(),
            ProcessingStage::Transform,
            original_data,
            error_type,
            error_message,
        )
        .with_attempt(attempt_number);

        // Mark as retryable if it's a transient error
        if matches!(error.error_type(), ErrorType::Transient) {
            failed_row = failed_row.with_retryable(true);
        }

        // Add error details for specific error types
        match &error {
            TransformError::ValidationFailed { rule, message } => {
                failed_row =
                    failed_row.with_error_details(format!("Rule: {}, Message: {}", rule, message));
            }
            _ => {}
        }

        failed_row
    }

    /// Check if failed rows should be tracked based on configuration
    pub fn should_track_failed_rows(&self) -> bool {
        self.error_handling
            .as_ref()
            .and_then(|eh| eh.failed_rows.as_ref())
            .map(|fr| !matches!(fr.action, FailedRowsAction::Skip))
            .unwrap_or(false)
    }

    pub fn get_failed_rows_config(&self) -> Option<&FailedRowsConfig> {
        self.error_handling
            .as_ref()
            .and_then(|eh| eh.failed_rows.as_ref())
    }

    fn row_to_hashmap(&self, row: &RowData) -> HashMap<String, Value> {
        row.field_values
            .iter()
            .map(|fv| (fv.name.clone(), fv.value.clone().unwrap_or(Value::Null)))
            .collect()
    }

    fn build_retry_policy(config: &RetryConfig) -> RetryPolicy {
        let base_delay = Duration::from_millis(config.delay_ms);
        let max_delay = match config.backoff {
            BackoffStrategy::Fixed => base_delay,
            BackoffStrategy::Linear => base_delay * config.max_attempts,
            BackoffStrategy::Exponential => base_delay * (1 << config.max_attempts.min(10)),
        };

        RetryPolicy::new(config.max_attempts as usize, base_delay, max_delay)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use model::{
        core::{data_type::DataType, value::FieldValue},
        execution::pipeline::{FailedRowsConfig, FailedRowsDestination, FileFormat},
    };

    fn create_test_row() -> RowData {
        let field_values = vec![
            FieldValue {
                name: "user_id".to_string(),
                value: Some(Value::Uint(123)),
                data_type: DataType::LongLong,
            },
            FieldValue {
                name: "email".to_string(),
                value: Some(Value::String("test@example.com".to_string())),
                data_type: DataType::VarChar,
            },
        ];
        RowData::new("test_entity", field_values)
    }

    #[tokio::test]
    async fn test_execute_success() {
        let executor = TransformRetryExecutor::new("test_pipeline".to_string(), None);
        let mut row = create_test_row();

        let result = executor.execute(&mut row, |_row| async { Ok(()) }).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_execute_permanent_failure() {
        let executor = TransformRetryExecutor::new("test_pipeline".to_string(), None);
        let mut row = create_test_row();

        let result = executor
            .execute(&mut row, |_row| async {
                Err(TransformError::Transformation("Invalid data".to_string()))
            })
            .await;

        assert!(result.is_err());
        let failed_row = result.unwrap_err();
        assert_eq!(failed_row.pipeline_name, "test_pipeline");
        assert_eq!(failed_row.stage, ProcessingStage::Transform);
        assert!(!failed_row.error.is_retryable);
    }

    #[tokio::test]
    async fn test_execute_transient_failure_then_success() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };

        let attempts = Arc::new(AtomicUsize::new(0));
        let error_handling = ErrorHandling {
            retry: Some(RetryConfig {
                max_attempts: 3,
                delay_ms: 0,
                backoff: BackoffStrategy::Fixed,
            }),
            failed_rows: None,
        };

        let executor =
            TransformRetryExecutor::new("test_pipeline".to_string(), Some(error_handling));
        let mut row = create_test_row();
        let attempts_clone = attempts.clone();

        let result = executor
            .execute(&mut row, |_row| {
                let attempts = attempts_clone.clone();
                async move {
                    let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                    if attempt < 2 {
                        Err(TransformError::NetworkError(
                            "Temporary failure".to_string(),
                        ))
                    } else {
                        Ok(())
                    }
                }
            })
            .await;

        assert!(result.is_ok());
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_execute_exhausted_retries() {
        let error_handling = ErrorHandling {
            retry: Some(RetryConfig {
                max_attempts: 2,
                delay_ms: 0,
                backoff: BackoffStrategy::Fixed,
            }),
            failed_rows: None,
        };

        let executor =
            TransformRetryExecutor::new("test_pipeline".to_string(), Some(error_handling));
        let mut row = create_test_row();

        let result = executor
            .execute(&mut row, |_row| async {
                Err(TransformError::NetworkError(
                    "Persistent network issue".to_string(),
                ))
            })
            .await;

        assert!(result.is_err());
        let failed_row = result.unwrap_err();
        assert_eq!(failed_row.attempt_number, Some(2));
        assert!(failed_row.error.is_retryable);
    }

    #[test]
    fn test_should_track_failed_rows() {
        let error_handling = ErrorHandling {
            retry: None,
            failed_rows: Some(FailedRowsConfig {
                action: FailedRowsAction::SaveToTable,
                destination: Some(FailedRowsDestination::File {
                    path: "/tmp/failed.json".to_string(),
                    format: FileFormat::Json,
                }),
            }),
        };

        let executor =
            TransformRetryExecutor::new("test_pipeline".to_string(), Some(error_handling));
        assert!(executor.should_track_failed_rows());
    }

    #[test]
    fn test_should_not_track_failed_rows_when_skip() {
        let error_handling = ErrorHandling {
            retry: None,
            failed_rows: Some(FailedRowsConfig {
                action: FailedRowsAction::Skip,
                destination: None,
            }),
        };

        let executor =
            TransformRetryExecutor::new("test_pipeline".to_string(), Some(error_handling));
        assert!(!executor.should_track_failed_rows());
    }

    #[test]
    fn test_retry_policy_exponential_backoff() {
        let retry_config = RetryConfig {
            max_attempts: 5,
            delay_ms: 100,
            backoff: BackoffStrategy::Exponential,
        };

        let policy = TransformRetryExecutor::build_retry_policy(&retry_config);
        assert_eq!(policy.max_attempts, 5);
        assert_eq!(policy.base_delay, Duration::from_millis(100));
    }
}
