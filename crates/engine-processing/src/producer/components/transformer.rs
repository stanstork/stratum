use crate::transform::pipeline::TransformPipeline;
use futures::{StreamExt, stream};
use model::records::row::RowData;
use std::num::NonZeroUsize;

/// Handles concurrent transformation of rows.
pub struct TransformService {
    pipeline: TransformPipeline,
    concurrency: NonZeroUsize,
}

impl TransformService {
    pub fn new(pipeline: TransformPipeline, concurrency: NonZeroUsize) -> Self {
        Self {
            pipeline,
            concurrency,
        }
    }

    /// Apply transformations to a batch of rows concurrently.
    pub async fn transform(&self, rows: Vec<RowData>) -> Vec<RowData> {
        stream::iter(rows.into_iter().map(|row| {
            let transform_pipeline = &self.pipeline;
            async move { transform_pipeline.apply(&row) }
        }))
        .buffer_unordered(self.concurrency.get())
        .collect()
        .await
    }

    /// Transform rows and collect errors separately
    pub async fn transform_with_errors(
        &self,
        rows: Vec<RowData>,
    ) -> (Vec<RowData>, Vec<(RowData, String)>) {
        let results: Vec<Result<RowData, (RowData, String)>> =
            stream::iter(rows.into_iter().map(|row| {
                let transform_pipeline = self.pipeline.clone();
                let input_row = row.clone();
                async move {
                    let output = transform_pipeline.apply(&row);
                    if output.field_values.is_empty() {
                        Err((input_row, "Transform produced empty row".to_string()))
                    } else {
                        Ok(output)
                    }
                }
            }))
            .buffer_unordered(self.concurrency.get())
            .collect()
            .await;

        let mut success = Vec::new();
        let mut errors = Vec::new();

        for result in results {
            match result {
                Ok(row) => success.push(row),
                Err(e) => errors.push(e),
            }
        }

        (success, errors)
    }

    pub fn pipeline(&self) -> &TransformPipeline {
        &self.pipeline
    }
}
