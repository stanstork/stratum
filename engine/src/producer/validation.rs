use crate::{
    producer::DataProducer,
    report::validation::{
        DryRunStatus, Finding, FindingKind, Severity, SqlKind, SqlStatement, TransformationRecord,
        TransformationReport,
    },
    source::{data::DataSource, Source},
    state::MigrationState,
    transform::pipeline::TransformPipeline,
};
use async_trait::async_trait;
use query_builder::dialect::{self, Dialect};
use sql_adapter::query::generator::QueryGenerator;
use std::sync::Arc;
use tokio::sync::Mutex;

/// The producer for a validation run. Fetches a small sample, transforms it,
/// and writes the results and diagnostics to the ValidationReport.
pub struct ValidationProducer {
    state: Arc<Mutex<MigrationState>>,
    source: Source,
    pipeline: TransformPipeline,
    sample_size: usize,
}

impl ValidationProducer {
    pub fn new(
        state: Arc<Mutex<MigrationState>>,
        source: Source,
        pipeline: TransformPipeline,
        sample_size: usize,
    ) -> Self {
        Self {
            state,
            source,
            pipeline,
            sample_size,
        }
    }
}

#[async_trait]
impl DataProducer for ValidationProducer {
    async fn run(&mut self) -> usize {
        let dialect = dialect::MySql; // TODO: Determine dialect from source connection

        let statements: Vec<SqlStatement> = match &self.source.primary {
            DataSource::Database(db) => {
                let db = db.lock().await;
                let reqs = db.build_fetch_rows_requests(self.sample_size, None);
                let generator = QueryGenerator::new(&dialect);

                reqs.into_iter()
                    .map(|r| {
                        let (sql, params) = generator.select(&r);
                        SqlStatement {
                            dialect: dialect.name(),
                            kind: SqlKind::Data,
                            sql,
                            params,
                        }
                    })
                    .collect()
            }
            _ => {
                // Record unsupported source error and stop early
                {
                    let state = self.state.lock().await;
                    let mut report = state.dry_run_report.lock().await;
                    report.summary.errors.push(Finding {
                        code: "UNSUPPORTED_SOURCE".to_string(),
                        message: format!(
                            "Validation run does not support source type: {:?}",
                            self.source.format()
                        ),
                        severity: Severity::Error,
                        kind: FindingKind::SourceSchema,
                        location: None,
                        suggestion: Some("Use a database source for validation runs.".to_string()),
                    });
                }
                return 0;
            }
        };

        let state = self.state.lock().await;
        let mut report = state.dry_run_report.lock().await;

        let fetched = self.source.fetch_data(self.sample_size, None).await;
        let (records_sampled, transform_report, fetch_error): (
            usize,
            Option<TransformationReport>,
            Option<Finding>,
        ) = match fetched {
            Ok(data) => {
                let sample: Vec<TransformationRecord> = data
                    .iter()
                    .filter_map(|record| {
                        let input = record.to_row_data()?.clone();
                        let transformed = self.pipeline.apply(record);
                        Some(TransformationRecord {
                            input,
                            output: transformed.to_row_data().cloned(),
                            error: None,
                            warnings: None,
                        })
                    })
                    .collect();

                let ok = sample.len();
                let total = data.len();
                let failed = total.saturating_sub(ok);

                (
                    total,
                    Some(TransformationReport { ok, failed, sample }),
                    None,
                )
            }
            Err(e) => {
                let finding = Finding {
                    code: "FETCH_ERROR".to_string(),
                    message: format!("Error fetching data: {e}"),
                    severity: Severity::Error,
                    kind: FindingKind::SourceData,
                    location: None,
                    suggestion: Some("Check source connectivity and query validity.".to_string()),
                };
                (0, None, Some(finding))
            }
        };

        // append generated SQL
        report.generated_sql.statements.extend(statements);

        // set summary sample size
        report.summary.records_sampled = records_sampled;

        // set transform report or append fetch error
        if let Some(tr) = transform_report {
            report.transform = tr;
        }
        if let Some(err) = fetch_error {
            report.summary.errors.push(err);
        }

        report.summary.status = if report
            .summary
            .errors
            .iter()
            .any(|e| e.severity == Severity::Error)
        {
            DryRunStatus::Failure
        } else {
            DryRunStatus::Success
        };

        records_sampled
    }
}
