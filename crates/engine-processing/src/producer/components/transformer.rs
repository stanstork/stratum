use crate::transform::{failed_row_writer::FailedRowWriter, pipeline::TransformPipeline};
use engine_core::context::exec::ExecutionContext;
use model::{
    execution::{
        failed_row::{FailedRow, ProcessingStage},
        pipeline::ErrorHandling,
    },
    records::row::RowData,
};
use std::{collections::HashMap, sync::Arc};
use tracing::warn;

/// Handles transformation of rows with batch processing and failed row tracking.
/// Transforms fail fast - no retry at this level. Retry happens at load/write level.
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
    /// Batch processing continues even if individual rows fail.
    pub async fn transform(
        &self,
        rows: Vec<RowData>,
    ) -> Result<Vec<RowData>, crate::transform::error::TransformError> {
        let (successful, _filtered, _failed_rows, has_fatal) = self.transform_batch(rows).await;

        if has_fatal {
            // Validation failure detected - stop migration
            return Err(crate::transform::error::TransformError::ValidationFailed {
                rule: "pipeline_validation".to_string(),
                message: format!("Validation failures detected in batch (see DLQ for details)"),
            });
        }

        // Regular transformation errors were sent to DLQ - continue migration
        Ok(successful)
    }

    /// Transform a batch of rows with fail-fast semantics.
    /// Returns (successful_rows, filtered_rows, failed_rows, has_fatal_error).
    async fn transform_batch(
        &self,
        rows: Vec<RowData>,
    ) -> (Vec<RowData>, Vec<RowData>, Vec<FailedRow>, bool) {
        let mut successful = Vec::new();
        let mut filtered = Vec::new();
        let mut failed_rows = Vec::new();
        let mut has_fatal = false;

        for mut row in rows {
            // Apply pipeline - fail fast, no retry
            match self.pipeline.apply(&mut row) {
                Ok(true) => {
                    // Row transformed successfully
                    successful.push(row);
                }
                Ok(false) => {
                    // Row filtered out (not an error)
                    filtered.push(row);
                }
                Err(e) => {
                    // Check if this is a fatal error (validation failure)
                    if e.is_fatal() {
                        has_fatal = true;
                    }

                    // Transformation failed - create FailedRow for DLQ
                    let failed_row = self.create_failed_row(&row, e);
                    failed_rows.push(failed_row);
                    // Continue processing remaining rows in batch
                }
            }
        }

        // Write all failed rows to DLQ as a batch (more efficient)
        if !failed_rows.is_empty() {
            if let Some(writer) = &self.failed_row_writer {
                tracing::info!("Writing {} failed rows to DLQ", failed_rows.len());
                if let Err(write_err) = writer.write_batch(&failed_rows).await {
                    warn!(
                        "Failed to write {} failed rows to DLQ: {}",
                        failed_rows.len(),
                        write_err
                    );
                }
            } else {
                tracing::warn!(
                    "No DLQ writer configured, {} failed rows will not be written",
                    failed_rows.len()
                );
            }
        }

        (successful, filtered, failed_rows, has_fatal)
    }

    /// Create a FailedRow from a transformation error
    fn create_failed_row(
        &self,
        row: &RowData,
        error: crate::transform::error::TransformError,
    ) -> FailedRow {
        // Extract original data from row
        let original_data: HashMap<String, model::core::value::Value> = row
            .field_values
            .iter()
            .filter_map(|fv| fv.value.as_ref().map(|v| (fv.name.clone(), v.clone())))
            .collect();

        // Determine the processing stage based on error type
        let stage = match &error {
            crate::transform::error::TransformError::ValidationFailed { .. } => {
                ProcessingStage::Validation
            }
            _ => ProcessingStage::Transform,
        };

        let mut failed_row = FailedRow::new(
            self.pipeline_name.clone(),
            stage,
            original_data,
            format!("{:?}", error), // Error type
            error.to_string(),      // Error message
        );
        failed_row.table_name = Some(row.entity.clone());

        failed_row
    }

    /// Transform rows and collect errors and filtered rows separately.
    /// Returns (successful_rows, filtered_rows, failed_rows_with_errors).
    pub async fn transform_with_errors(
        &self,
        rows: Vec<RowData>,
    ) -> (Vec<RowData>, Vec<RowData>, Vec<(RowData, String)>) {
        let entity = if let Some(first_row) = rows.first() {
            first_row.entity.clone()
        } else {
            String::new()
        };
        let (successful, filtered, failed_rows, _has_fatal) = self.transform_batch(rows).await;

        let failed_with_strings: Vec<(RowData, String)> = failed_rows
            .into_iter()
            .map(|fr| {
                let row_data = fr.to_row_data(&entity);
                (row_data, fr.error.message)
            })
            .collect();

        (successful, filtered, failed_with_strings)
    }

    pub fn pipeline(&self) -> &TransformPipeline {
        &self.pipeline
    }
}
