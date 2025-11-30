use super::steps::{
    fast_path::FastPathValidationStep, sampling::SamplingStep, schema::SchemaValidationStep,
    sql_gen::SqlGenerationStep,
};
use crate::{
    error::ProducerError,
    producer::{DataProducer, ProducerStatus, validation::steps::sampling::SampleResult},
    transform::pipeline::TransformPipeline,
};
use async_trait::async_trait;
use connectors::sql::base::error::DbError;
use engine_config::{
    report::{
        dry_run::{DryRunReport, DryRunStatus, FastPathSummary, OffsetValidationReport},
        finding::{Finding, Severity},
        sql::SqlStatement,
        transform::TransformationReport,
    },
    settings::validated::ValidatedSettings,
};
use engine_core::connectors::{destination::Destination, source::Source};
use futures::lock::Mutex;
use model::{pagination::cursor::Cursor, transform::mapping::EntityMapping};
use planner::query::offsets::OffsetStrategy;
use smql_syntax::ast::setting::CopyColumns;
use std::sync::Arc;

pub struct ValidationProducer {
    report: Arc<Mutex<DryRunReport>>,
    source: Source,
    destination: Destination,
    pipeline: TransformPipeline,
    mapping: EntityMapping,
    settings: ValidatedSettings,
    offset_strategy: Arc<dyn OffsetStrategy>,
    cursor: Cursor,
    sample_size: usize,
}

pub struct ValidationProducerParams {
    pub source: Source,
    pub destination: Destination,
    pub pipeline: TransformPipeline,
    pub mapping: EntityMapping,
    pub settings: ValidatedSettings,
    pub offset_strategy: Arc<dyn OffsetStrategy>,
    pub cursor: Cursor,
    pub report: Arc<Mutex<DryRunReport>>,
}

impl ValidationProducer {
    pub fn new(params: ValidationProducerParams) -> Self {
        let ValidationProducerParams {
            source,
            destination,
            pipeline,
            mapping,
            settings,
            offset_strategy,
            cursor,
            report,
        } = params;

        ValidationProducer {
            report,
            source,
            destination,
            pipeline,
            mapping,
            settings,
            offset_strategy,
            cursor,
            sample_size: 10, // TODO: make configurable
        }
    }

    /// Execute all validation steps and update the report
    async fn perform_validation(&mut self) -> Result<(), ProducerError> {
        // Step 1: SQL Generation
        let sql_step =
            SqlGenerationStep::new(self.source.clone(), self.sample_size, self.cursor.clone());
        let (statements, prep_findings) = sql_step.generate_statements().await;

        // Step 2: Schema Validation & Sampling
        let schema_step = SchemaValidationStep::new(
            self.source.clone(),
            self.destination.clone(),
            self.mapping.clone(),
            self.settings.clone(),
        );
        let mut validator = schema_step.init_validator().await?;

        let sampling_step = SamplingStep::new(
            self.source.clone(),
            self.pipeline.clone(),
            self.mapping.clone(),
            self.settings.clone(),
            self.cursor.clone(),
            self.sample_size,
        );
        let sample_result = sampling_step.sample_and_transform(&mut validator).await?;

        let schema_validation_error = validator
            .validate_pending_keys(&self.destination)
            .await
            .err();
        let schema_findings = validator.findings();

        // Step 3: Fast Path Validation
        let fast_path_step = FastPathValidationStep::new(
            self.source.clone(),
            self.destination.clone(),
            self.mapping.clone(),
            self.settings.clone(),
        );
        let fast_path_summary = fast_path_step.eval_fast_path().await;

        // Update the report
        self.update_report(
            statements,
            prep_findings,
            sample_result,
            schema_findings,
            schema_validation_error,
            fast_path_summary,
        )
        .await;

        Ok(())
    }

    /// Update the dry run report with all validation results
    async fn update_report(
        &self,
        statements: Vec<SqlStatement>,
        prep_findings: Vec<Finding>,
        sample_result: SampleResult,
        schema_findings: Vec<Finding>,
        schema_validation_error: Option<DbError>,
        fast_path_summary: FastPathSummary,
    ) {
        let mut report = self.report.lock().await;

        // Update SQL statements
        report.generated_sql.statements.extend(statements);

        // Update records sampled
        report.summary.records_sampled = sample_result.records_sampled;

        // Handle schema validation error
        if let Some(ref e) = schema_validation_error {
            let error_msg = format!("Schema validation error: {e}");
            report
                .summary
                .errors
                .push(Finding::new_fetch_error(&error_msg));
        }

        // Collect all findings
        report.summary.errors.extend(prep_findings);

        // Update transformation report
        let ok = sample_result
            .transformation_records
            .iter()
            .filter(|r| r.output.is_some())
            .count();
        let failed = sample_result.records_sampled.saturating_sub(ok);
        report.transform = TransformationReport {
            ok,
            failed,
            sample: sample_result.transformation_records.clone(),
        };

        // Update mapping omissions
        for entity_report in &mut report.mapping.entities {
            if let Some(omitted) = sample_result
                .omitted_columns
                .get(&entity_report.source_entity)
            {
                entity_report.omitted_source_columns.extend(omitted.clone());
            }

            // Update one-to-one mapped columns if needed
            if self.settings.copy_columns == CopyColumns::All {
                let schema_step = SchemaValidationStep::new(
                    self.source.clone(),
                    self.destination.clone(),
                    self.mapping.clone(),
                    self.settings.clone(),
                );
                let _ = schema_step.update_one_to_one_mapped(entity_report).await;
            }
        }

        // Update schema validation findings
        report.schema_validation.findings.extend(schema_findings);

        // Update offset validation
        report.offset_validation = self.build_offset_validation_report(&sample_result);

        // Update fast path summary
        report.fast_path_summary = fast_path_summary;

        // Calculate final status
        report.summary.status = Self::calculate_status(&report);
    }

    fn calculate_status(report: &DryRunReport) -> DryRunStatus {
        let has_errors = report
            .summary
            .errors
            .iter()
            .any(|e| e.severity == Severity::Error)
            || report
                .schema_validation
                .findings
                .iter()
                .any(|f| f.severity == Severity::Error);

        let has_warnings = report
            .mapping
            .entities
            .iter()
            .any(|e| !e.warnings.is_empty())
            || report
                .transform
                .sample
                .iter()
                .any(|r| r.warnings.as_ref().is_some_and(|w| !w.is_empty()));

        if has_errors {
            DryRunStatus::Failure
        } else if has_warnings {
            DryRunStatus::SuccessWithWarnings
        } else {
            DryRunStatus::Success
        }
    }

    fn build_offset_validation_report(&self, sample: &SampleResult) -> OffsetValidationReport {
        let strategy = self.offset_strategy.name();

        let key_columns = match &self.cursor {
            Cursor::Pk { pk_col, .. } => vec![pk_col.column.clone()],
            Cursor::CompositeNumPk {
                num_col, pk_col, ..
            } => {
                vec![num_col.column.clone(), pk_col.column.clone()]
            }
            Cursor::CompositeTsPk { ts_col, pk_col, .. } => {
                vec![ts_col.column.clone(), pk_col.column.clone()]
            }
            Cursor::Default { .. } | Cursor::None => Vec::new(),
            _ => vec!["<complex cursor>".to_string()],
        };

        OffsetValidationReport {
            strategy,
            initial_cursor: Some(self.cursor.clone()),
            last_cursor: sample.next_cursor.clone(),
            key_columns,
            source_entity: sample.source_entity.clone(),
            rows_fetched: Some(sample.records_sampled),
            reached_end: Some(sample.reached_end),
            findings: Vec::new(),
        }
    }
}

#[async_trait]
impl DataProducer for ValidationProducer {
    async fn start_snapshot(&mut self) -> Result<(), ProducerError> {
        self.perform_validation().await
    }

    async fn start_cdc(&mut self) -> Result<(), ProducerError> {
        // CDC not supported in validation mode
        Ok(())
    }

    async fn resume(
        &mut self,
        _run_id: &str,
        _item_id: &str,
        _part_id: &str,
    ) -> Result<(), ProducerError> {
        // Resume not supported in validation mode
        Ok(())
    }

    async fn tick(&mut self) -> Result<ProducerStatus, ProducerError> {
        // Validation is done in start_snapshot, tick just returns Finished
        Ok(ProducerStatus::Finished)
    }

    async fn stop(&mut self) -> Result<(), ProducerError> {
        Ok(())
    }

    fn rows_produced(&self) -> u64 {
        0
    }
}
