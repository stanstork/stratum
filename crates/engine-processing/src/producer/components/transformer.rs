use crate::transform::pipeline::TransformPipeline;
use model::records::row::RowData;
use std::num::NonZeroUsize;

/// Handles transformation of rows with batch processing.
pub struct TransformService {
    pipeline: TransformPipeline,
    _concurrency: NonZeroUsize,
}

impl TransformService {
    pub fn new(pipeline: TransformPipeline, concurrency: NonZeroUsize) -> Self {
        Self {
            pipeline,
            _concurrency: concurrency,
        }
    }

    /// Apply transformations to a batch of rows.
    /// Returns only successfully transformed rows.
    pub async fn transform(&self, rows: Vec<RowData>) -> Vec<RowData> {
        let (successful, _filtered, _failed) = self.pipeline.apply_batch(rows);
        successful
    }

    /// Transform rows and collect errors and filtered rows separately.
    /// Returns (successful_rows, filtered_rows, failed_rows_with_errors).
    pub async fn transform_with_errors(
        &self,
        rows: Vec<RowData>,
    ) -> (Vec<RowData>, Vec<RowData>, Vec<(RowData, String)>) {
        let (successful, filtered, failed) = self.pipeline.apply_batch(rows);

        let failed_with_strings: Vec<(RowData, String)> = failed
            .into_iter()
            .map(|(row, err)| (row, err.to_string()))
            .collect();

        (successful, filtered, failed_with_strings)
    }

    pub fn pipeline(&self) -> &TransformPipeline {
        &self.pipeline
    }
}
