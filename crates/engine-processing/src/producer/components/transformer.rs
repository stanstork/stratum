use crate::transform::{
    error::TransformError, failed_row_writer::FailedRowWriter, pipeline::TransformPipeline,
    retry::TransformRetryExecutor,
};
use engine_core::context::exec::ExecutionContext;
use model::{
    execution::{failed_row::FailedRow, pipeline::ErrorHandling},
    records::row::RowData,
};
use std::sync::Arc;
use tracing::warn;

/// Handles transformation of rows with batch processing and retry logic.
pub struct TransformService {
    pipeline: TransformPipeline,
    retry_executor: Option<TransformRetryExecutor>,
    failed_row_writer: Option<FailedRowWriter>,
}

impl TransformService {
    pub fn new(
        ctx: Arc<ExecutionContext>,
        pipeline: TransformPipeline,
        pipeline_name: String,
        error_handling: Option<ErrorHandling>,
    ) -> Self {
        let (retry_executor, failed_row_writer) = if let Some(ref eh) = error_handling {
            let retry_exec =
                TransformRetryExecutor::new(pipeline_name.clone(), error_handling.clone());

            let writer = eh.failed_rows.as_ref().and_then(|fr_config| {
                fr_config
                    .destination
                    .as_ref()
                    .map(|dest| FailedRowWriter::new(dest.clone(), ctx.clone()))
            });

            (Some(retry_exec), writer)
        } else {
            (None, None)
        };

        Self {
            pipeline,
            retry_executor,
            failed_row_writer,
        }
    }

    /// Apply transformations to a batch of rows with retry logic.
    /// Returns only successfully transformed rows.
    pub async fn transform(&self, rows: Vec<RowData>) -> Vec<RowData> {
        let (successful, _filtered, _failed) = self.transform_with_retry(rows).await;
        successful
    }

    /// Transform rows with retry and failed row tracking.
    /// Returns (successful_rows, filtered_rows, failed_rows).
    async fn transform_with_retry(
        &self,
        rows: Vec<RowData>,
    ) -> (Vec<RowData>, Vec<RowData>, Vec<FailedRow>) {
        let mut successful = Vec::new();
        let mut filtered = Vec::new();
        let mut failed_rows = Vec::new();

        for mut row in rows {
            if let Some(retry_exec) = &self.retry_executor {
                let result = retry_exec
                    .execute(&mut row, |r| {
                        let apply_result = self.pipeline.apply(r);
                        async move {
                            match apply_result {
                                Ok(true) => Ok(()),
                                Ok(false) => Err(TransformError::FilteredOut),
                                Err(e) => Err(e),
                            }
                        }
                    })
                    .await;

                match result {
                    Ok(()) => successful.push(row),
                    Err(failed_row) => {
                        // Write failed row if writer is configured
                        if let Some(writer) = &self.failed_row_writer {
                            if let Err(e) = writer.write(&failed_row).await {
                                warn!("Failed to write failed row: {}", e);
                            }
                        }
                        failed_rows.push(failed_row);
                    }
                }
            } else {
                // No retry configured - use standard pipeline behavior
                match self.pipeline.apply(&mut row) {
                    Ok(true) => successful.push(row),
                    Ok(false) => filtered.push(row),
                    Err(_) => {
                        filtered.push(row);
                    }
                }
            }
        }

        (successful, filtered, failed_rows)
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
        let (successful, filtered, failed_rows) = self.transform_with_retry(rows).await;

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
