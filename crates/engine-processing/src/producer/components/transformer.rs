use crate::transform::{
    error::{ErrorType, TransformError},
    failed_row_writer::FailedRowWriter,
    pipeline::{BatchOutput, TransformPipeline},
};
use engine_core::context::exec::ExecutionContext;
use model::{
    execution::{
        failed_row::{FailedRow, ProcessingStage},
        pipeline::ErrorHandling,
    },
    records::Record,
};
use std::{collections::HashMap, sync::Arc};
use tracing::{debug, info, warn};

/// Result of transforming a batch of rows, including statistics
#[derive(Debug, Clone)]
pub struct TransformResult {
    /// Successfully transformed rows
    pub rows: Vec<Record>,
    /// Number of rows filtered/skipped during transformation
    pub rows_skipped: u64,
    /// Number of rows that failed transformation
    pub rows_failed: u64,
}

/// Handles transformation of rows with batch processing and failed row tracking.
pub struct TransformService {
    pipeline: TransformPipeline,
    pipeline_name: String,
    failed_row_writer: Option<FailedRowWriter>,
}

impl TransformService {
    pub fn new(
        ctx: Arc<ExecutionContext>,
        pipeline: TransformPipeline,
        pipeline_name: String,
        error_handling: Option<ErrorHandling>,
    ) -> Self {
        let failed_row_writer = error_handling.as_ref().and_then(|eh| {
            eh.failed_rows.as_ref().and_then(|fr_config| {
                fr_config
                    .destination
                    .as_ref()
                    .map(|dest| FailedRowWriter::new(dest.clone(), ctx.clone()))
            })
        });

        Self {
            pipeline,
            pipeline_name,
            failed_row_writer,
        }
    }

    /// Apply transformations to a batch of rows.
    /// - Data/transformation errors: sent to DLQ, migration continues
    /// - Validation failures: sent to DLQ, migration stops (indicates bad pipeline config)
    ///
    /// Batch processing continues even if individual rows fail.
    pub async fn transform(
        &self,
        run_id: &str,
        batch_id: &str,
        rows: Vec<Record>,
    ) -> Result<TransformResult, TransformError> {
        let (successful, filtered, failed_rows, has_fatal) =
            self.transform_batch(run_id, batch_id, rows).await;

        if has_fatal {
            // Validation failure detected - stop migration
            return Err(TransformError::ValidationFailed {
                rule: "pipeline_validation".to_string(),
                message: "Validation failures detected in batch (see DLQ for details)".to_string(),
            });
        }

        // Regular transformation errors were sent to DLQ - continue migration
        Ok(TransformResult {
            rows: successful,
            rows_skipped: filtered.len() as u64,
            rows_failed: failed_rows.len() as u64,
        })
    }

    /// Transform a batch of rows with fail-fast semantics.
    /// Returns (successful_rows, filtered_rows, failed_rows, has_fatal_error).
    async fn transform_batch(
        &self,
        run_id: &str,
        batch_id: &str,
        rows: Vec<Record>,
    ) -> (Vec<Record>, Vec<Record>, Vec<FailedRow>, bool) {
        // Cap the number of error messages we retain.
        const MAX_ERROR_SAMPLES: usize = 10;

        // Run the whole batch through the pipeline stage-by-stage.
        let BatchOutput {
            successful,
            filtered,
            failed,
        } = self.pipeline.run_batch(rows);

        let mut failed_rows = Vec::with_capacity(failed.len());
        let mut error_samples = Vec::new();
        let mut has_fatal = false;

        for (row, error) in &failed {
            // Check if this is a fatal error (validation failure).
            if error.is_fatal() {
                has_fatal = true;
            }

            // The error is otherwise only captured inside the FailedRow. Log it
            // so the cause is diagnosable.
            let err_msg = error.to_string();
            debug!(
                pipeline = %self.pipeline_name,
                batch_id = %batch_id,
                error = %err_msg,
                "row transformation failed"
            );

            if error_samples.len() < MAX_ERROR_SAMPLES {
                error_samples.push(err_msg);
            }

            failed_rows.push(self.create_failed_row(run_id, batch_id, row, error));
        }

        if !failed_rows.is_empty() {
            let sample = summarize_errors(&error_samples);
            if let Some(writer) = &self.failed_row_writer {
                info!(count = failed_rows.len(), sample = %sample, "writing failed rows to DLQ");
                if let Err(write_err) = writer.write_batch(&failed_rows).await {
                    warn!(
                        count = failed_rows.len(),
                        error = %write_err,
                        "failed to write rows to DLQ"
                    );
                }
            } else {
                warn!(
                    count = failed_rows.len(),
                    causes = %sample,
                    "no DLQ writer configured, failed rows will be dropped"
                );
            }
        }

        (successful, filtered, failed_rows, has_fatal)
    }

    fn create_failed_row(
        &self,
        run_id: &str,
        batch_id: &str,
        row: &Record,
        error: &TransformError,
    ) -> FailedRow {
        let stage = match error {
            TransformError::ValidationFailed { .. } => ProcessingStage::Validation,
            _ => ProcessingStage::Transform,
        };
        let is_retryable = matches!(error.error_type(), ErrorType::Transient);

        FailedRow::new(
            self.pipeline_name.clone(),
            stage,
            row.to_map(),
            format!("{:?}", error), // Error type
            error.to_string(),      // Error message
        )
        .with_execution_context(run_id.to_string(), Some(batch_id.to_string()), None)
        .with_table(row.table().to_string())
        .with_retryable(is_retryable)
    }

    pub fn pipeline(&self) -> &TransformPipeline {
        &self.pipeline
    }
}

/// Build a compact, human-readable summary of failed-row error messages.
fn summarize_errors(messages: &[String]) -> String {
    const MAX_DISTINCT: usize = 3;

    let mut order: Vec<&str> = Vec::new();
    let mut counts: HashMap<&str, usize> = HashMap::new();

    for msg in messages {
        let entry = counts.entry(msg.as_str()).or_insert(0);
        if *entry == 0 {
            order.push(msg.as_str());
        }
        *entry += 1;
    }

    let shown: Vec<String> = order
        .iter()
        .take(MAX_DISTINCT)
        .map(|msg| {
            let count = counts[msg];
            if count > 1 {
                format!("{msg} (x{count})")
            } else {
                (*msg).to_string()
            }
        })
        .collect();

    let mut summary = shown.join("; ");
    if order.len() > MAX_DISTINCT {
        summary.push_str(&format!(
            "; … +{} more distinct",
            order.len() - MAX_DISTINCT
        ));
    }
    summary
}
