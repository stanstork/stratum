use crate::{
    context::item::ItemContext,
    producer::{create_pipeline_from_context, DataProducer},
    report::validation::{TransformationRecord, TransformationSummary},
    source::{data::DataSource, Source},
    state::MigrationState,
    transform::pipeline::TransformPipeline,
};
use async_trait::async_trait;
use query_builder::dialect;
use sql_adapter::query::generator::QueryGenerator;
use std::sync::Arc;
use tokio::sync::{watch::Sender, Mutex};

/// The producer for a validation run. Fetches a small sample, transforms it,
/// and writes the results and diagnostics to the ValidationReport.
pub struct ValidationProducer {
    state: Arc<Mutex<MigrationState>>,
    source: Source,
    pipeline: TransformPipeline,
    shutdown_sender: Sender<bool>,
    sample_size: usize,
}

impl ValidationProducer {
    pub async fn new(ctx: &ItemContext, sender: Sender<bool>) -> Self {
        let state = ctx.state.clone();
        let source = ctx.source.clone();
        let pipeline = create_pipeline_from_context(ctx);
        let sample_size = 10; // TODO: Make this configurable

        Self {
            state,
            source,
            pipeline,
            shutdown_sender: sender,
            sample_size,
        }
    }
}

#[async_trait]
impl DataProducer for ValidationProducer {
    async fn run(&mut self) -> usize {
        let state = self.state.lock().await;
        let mut report = state.validation_report.lock().await;

        match &self.source.primary {
            DataSource::Database(db) => {
                let db = db.lock().await;
                let req = db.build_fetch_rows_requests(self.sample_size, None);
                let generator = QueryGenerator::new(&dialect::MySql);
                for r in req {
                    let query = generator.select(&r);
                    report
                        .generated_queries
                        .data_queries
                        .push((query.0, Some(query.1)));
                }
            }
            _ => {
                report
                    .summary
                    .errors
                    .push("Source is not a database".to_string());
                return 0;
            }
        }

        let fetched = self.source.fetch_data(self.sample_size, None).await;

        match fetched {
            Ok(data) => {
                let successful_transforms: Vec<TransformationRecord> = data
                    .iter()
                    .filter_map(|record| {
                        let input = record.to_row_data()?.clone();
                        let transformed = self.pipeline.apply(record);
                        Some(TransformationRecord {
                            input_record: input,
                            output_record: transformed.to_row_data().cloned(),
                            error: None,
                        })
                    })
                    .collect();

                let success = successful_transforms.len();
                let total = data.len();
                let failed = total.saturating_sub(success);

                report.transformation_summary = TransformationSummary {
                    successful_transformations: success,
                    failed_transformations: failed,
                    transformed_sample_data: successful_transforms,
                };
                report.summary.records_sampled = total;
            }
            Err(e) => {
                report
                    .summary
                    .errors
                    .push(format!("Error fetching data: {}", e));
            }
        }

        // Notify the consumer to shutdown
        self.shutdown_sender.send(true).unwrap();
        0
    }
}
