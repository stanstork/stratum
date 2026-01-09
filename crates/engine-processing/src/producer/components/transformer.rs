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
    records::row::RowData,
};
use std::sync::Arc;
use tracing::{info, warn};

/// Result of transforming a batch of rows, including statistics
#[derive(Debug, Clone)]
pub struct TransformResult {
    /// Successfully transformed rows
    pub rows: Vec<RowData>,
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
        rows: Vec<RowData>,
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
        rows: Vec<RowData>,
    ) -> (Vec<RowData>, Vec<RowData>, Vec<FailedRow>, bool) {
        let mut successful = Vec::new();
        let mut filtered = Vec::new();
        let mut failed_rows = Vec::new();
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

                    // Transformation failed - create FailedRow for DLQ
                    let failed_row = self.create_failed_row(run_id, batch_id, &row, e);
                    failed_rows.push(failed_row);
                    // Continue processing remaining rows in batch
                }
            }
        }

        if !failed_rows.is_empty() {
            if let Some(writer) = &self.failed_row_writer {
                info!("Writing {} failed rows to DLQ", failed_rows.len());
                if let Err(write_err) = writer.write_batch(&failed_rows).await {
                    warn!(
                        "Failed to write {} failed rows to DLQ: {}",
                        failed_rows.len(),
                        write_err
                    );
                }
            } else {
                warn!(
                    "No DLQ writer configured, {} failed rows will not be written",
                    failed_rows.len()
                );
            }
        }

        (successful, filtered, failed_rows, has_fatal)
    }

    fn create_failed_row(
        &self,
        run_id: &str,
        batch_id: &str,
        row: &RowData,
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
        .with_table(row.entity.clone())
        .with_retryable(is_retryable)
    }

    pub fn pipeline(&self) -> &TransformPipeline {
        &self.pipeline
    }
}
