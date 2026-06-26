use crate::transform::{
    error::{ErrorType, TransformError},
    failed_row_writer::FailedRowWriter,
    pipeline::{ApplyOutcome, TransformPipeline},
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

        let mut successful = Vec::new();
        let mut filtered = Vec::new();
        let mut failed_rows = Vec::new();
        let mut error_samples = Vec::new();
        let mut has_fatal = false;

        for mut row in rows {
            // Apply pipeline - fail fast, no retry
            match self.pipeline.apply(&mut row) {
                Ok(ApplyOutcome::Success) | Ok(ApplyOutcome::Warning { .. }) => {
                    // Row transformed successfully (warnings are non-fatal)
                    successful.push(row);
                }
                Ok(ApplyOutcome::Skipped { .. }) => {
                    // Row filtered out (not an error)
                    filtered.push(row);
                }
                Err(e) => {
                    // Check if this is a fatal error (validation failure)
                    if e.is_fatal() {
                        has_fatal = true;
                    }

                    // The error is otherwise only captured inside the FailedRow.
                    // Log it so the cause is diagnosable.
                    let err_msg = e.to_string();
                    debug!(
                        pipeline = %self.pipeline_name,
                        batch_id = %batch_id,
                        "row transformation failed: {err_msg}"
                    );

                    if error_samples.len() < MAX_ERROR_SAMPLES {
                        error_samples.push(err_msg);
                    }

                    // Transformation failed - create FailedRow for DLQ
                    let failed_row = self.create_failed_row(run_id, batch_id, &row, e);
                    failed_rows.push(failed_row);
                    // Continue processing remaining rows in batch
                }
            }
        }

        if !failed_rows.is_empty() {
            let sample = summarize_errors(&error_samples);
            if let Some(writer) = &self.failed_row_writer {
                info!(
                    "Writing {} failed rows to DLQ ({})",
                    failed_rows.len(),
                    sample
                );
                if let Err(write_err) = writer.write_batch(&failed_rows).await {
                    warn!(
                        "Failed to write {} failed rows to DLQ: {}",
                        failed_rows.len(),
                        write_err
                    );
                }
            } else {
                warn!(
                    "No DLQ writer configured, {} failed rows will not be written. Causes: {}",
                    failed_rows.len(),
                    sample
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
        error: TransformError,
    ) -> FailedRow {
        let stage = match &error {
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
        .with_table(row.schema.clone())
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
