use crate::{
    destination::Destination,
    error::ProducerError,
    producer::DataProducer,
    report::{
        dry_run::{DryRunReport, DryRunStatus},
        finding::{Finding, Severity},
        mapping::EntityMappingReport,
        sql::{SqlKind, SqlStatement},
        transform::{TransformationRecord, TransformationReport},
    },
    source::{data::DataSource, Source},
    state::MigrationState,
    transform::pipeline::TransformPipeline,
    validation::schema_validator::DestinationSchemaValidator,
};
use async_trait::async_trait;
use common::{mapping::EntityMapping, row_data::RowData};
use smql::statements::setting::{CopyColumns, Settings};
use sql_adapter::{error::db::DbError, query::generator::QueryGenerator};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tokio::sync::Mutex;

/// A container for the results of the `sample_and_transform` operation.
struct SampleResult {
    records_sampled: usize,
    transform_report: Option<TransformationReport>,
    fetch_error: Option<Finding>,
    prune_findings: Vec<Finding>,
    omitted_columns: HashMap<String, HashSet<String>>,
}

struct ValidationResults {
    statements: Vec<SqlStatement>,
    prep_findings: Vec<Finding>,
    sample_result: SampleResult,
    schema_findings: Vec<Finding>,
    schema_validation_error: Option<DbError>,
}

pub struct ValidationProducer {
    state: Arc<Mutex<MigrationState>>,
    source: Source,
    destination: Destination,
    pipeline: TransformPipeline,
    mapping: EntityMapping,
    settings: Settings,
}

impl ValidationProducer {
    pub fn new(
        state: Arc<Mutex<MigrationState>>,
        source: Source,
        destination: Destination,
        pipeline: TransformPipeline,
        mapping: EntityMapping,
        settings: Settings,
    ) -> Self {
        Self {
            state,
            source,
            destination,
            mapping,
            pipeline,
            settings,
        }
    }

    async fn perform_validation(&mut self) -> Result<ValidationResults, ProducerError> {
        let mut validator = DestinationSchemaValidator::new(
            &self.destination,
            self.mapping.clone(),
            &self.settings,
        )
        .await
        .map_err(|e| ProducerError::Other(format!("Init schema validator: {e}")))?;

        let (statements, prep_findings) = self.generate_sql_statements().await;
        let sample_result = self.sample_and_transform(&mut validator).await;

        let schema_validation_error = validator
            .validate_pending_keys(&self.destination)
            .await
            .err();

        Ok(ValidationResults {
            statements,
            prep_findings,
            sample_result,
            schema_findings: validator.findings(),
            schema_validation_error,
        })
    }

    async fn update_report(&self, results: ValidationResults) {
        let state = self.state.lock().await;
        let dry_run_report = state.dry_run_report();
        let mut report = dry_run_report.lock().await;

        report.generated_sql.statements.extend(results.statements);
        report.summary.records_sampled = results.sample_result.records_sampled;
        if let Some(tr) = results.sample_result.transform_report {
            report.transform = tr;
        }

        if let Some(e) = results.schema_validation_error {
            let error_msg = format!("Schema validation error: {e}");
            report
                .summary
                .errors
                .push(Finding::new_fetch_error(&error_msg));
        }
        report.summary.errors.extend(results.prep_findings);
        report
            .summary
            .errors
            .extend(results.sample_result.prune_findings);
        if let Some(err) = results.sample_result.fetch_error {
            report.summary.errors.push(err);
        }

        for entity_report in &mut report.mapping.entities {
            if let Some(omitted) = results
                .sample_result
                .omitted_columns
                .get(&entity_report.source_entity)
            {
                entity_report.omitted_source_columns.extend(omitted.clone());
            }

            if self.settings.copy_columns == CopyColumns::All {
                self.update_one_to_one_mapped(entity_report).await;
            }
        }

        report
            .schema_validation
            .findings
            .extend(results.schema_findings);
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

    fn is_allowed_output_field(&self, table: &str, field_name: &str) -> Result<bool, Finding> {
        if self.settings.copy_columns == CopyColumns::All {
            return Ok(true);
        }

        let colmap = self
            .mapping
            .field_mappings
            .column_mappings
            .get(table)
            .ok_or_else(|| Finding::new_mapping_missing(table, ""))?;

        if colmap.contains_target_key(field_name) {
            return Ok(true);
        }

        if let Some(computed_fields) = self.mapping.field_mappings.get_computed(table) {
            if computed_fields
                .iter()
                .any(|cf| cf.name.eq_ignore_ascii_case(field_name))
            {
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Prune unmapped columns when CopyColumns::MapOnly is set.
    fn prune_row(
        &self,
        row: &mut RowData,
        findings: &mut Vec<Finding>,
        omitted: &mut HashMap<String, HashSet<String>>,
    ) {
        if self.settings.copy_columns == CopyColumns::All {
            return;
        }

        let table = row.entity.as_str();

        // If no mapping exists for the table, we cannot determine which columns to keep.
        // Record a single finding and remove all fields.
        if !self
            .mapping
            .field_mappings
            .column_mappings
            .contains_key(table)
        {
            findings.push(Finding::new_mapping_missing(
                table,
                " Output row will be empty.",
            ));
            let omitted_for_table = omitted.entry(table.to_string()).or_default();
            for fv in row.field_values.drain(..) {
                omitted_for_table.insert(fv.name);
            }
            return;
        }

        // Partition the fields into retained and dropped lists.
        let (retained, dropped): (Vec<_>, Vec<_>) = row.field_values.drain(..).partition(|fv| {
            match self.is_allowed_output_field(table, &fv.name) {
                Ok(true) => true,
                Ok(false) => false,
                Err(finding) => {
                    findings.push(finding);
                    false
                }
            }
        });

        if !dropped.is_empty() {
            omitted
                .entry(table.to_string())
                .or_default()
                .extend(dropped.into_iter().map(|fv| fv.name));
        }

        row.field_values = retained;
    }

    async fn sample_and_transform(
        &self,
        validator: &mut DestinationSchemaValidator,
    ) -> SampleResult {
        let mut prune_findings = Vec::new();
        let mut omitted_columns: HashMap<String, HashSet<String>> = HashMap::new();

        match self.source.fetch_data(self.sample_size(), None).await {
            Ok(data) => {
                let total = data.len();
                let sample: Vec<TransformationRecord> = data
                    .into_iter()
                    .filter_map(|record| {
                        let input_row = record.to_row_data().cloned()?;
                        let transformed = self.pipeline.apply(&record);
                        let mut output_row_opt = transformed.to_row_data().cloned();

                        if let Some(ref mut output_row) = output_row_opt {
                            if output_row.entity.is_empty() {
                                output_row.entity = input_row.entity.clone();
                            }
                            self.prune_row(output_row, &mut prune_findings, &mut omitted_columns);
                            validator.validate(output_row);
                        }

                        Some(TransformationRecord {
                            input: input_row,
                            output: output_row_opt,
                            error: None,
                            warnings: None,
                        })
                    })
                    .collect();

                let ok = sample.iter().filter(|r| r.output.is_some()).count();
                let failed = total.saturating_sub(ok);
                let report = TransformationReport { ok, failed, sample };

                SampleResult {
                    records_sampled: total,
                    transform_report: Some(report),
                    fetch_error: None,
                    prune_findings,
                    omitted_columns,
                }
            }
            Err(e) => SampleResult {
                records_sampled: 0,
                transform_report: None,
                fetch_error: Some(Finding::new_fetch_error(&e.to_string())),
                prune_findings,
                omitted_columns,
            },
        }
    }

    async fn generate_sql_statements(&self) -> (Vec<SqlStatement>, Vec<Finding>) {
        match &self.source.primary {
            DataSource::Database(db_arc) => {
                let db = db_arc.lock().await;
                let dialect = self.source.dialect();
                let generator = QueryGenerator::new(dialect.as_ref());

                let requests = db.build_fetch_rows_requests(self.sample_size(), None);
                let statements = requests
                    .into_iter()
                    .map(|req| {
                        let (sql, params) = generator.select(&req);
                        SqlStatement {
                            dialect: dialect.name(),
                            kind: SqlKind::Data,
                            sql,
                            params,
                        }
                    })
                    .collect();
                (statements, Vec::new())
            }
            _ => (
                Vec::new(),
                Vec::new(), // No SQL statements for non-database sources
            ),
        }
    }

    async fn update_one_to_one_mapped(&self, entity_report: &mut EntityMappingReport) {
        if let Ok(meta) = self
            .source
            .primary
            .fetch_meta(entity_report.source_entity.clone())
            .await
        {
            let source_columns: HashSet<String> =
                meta.columns().iter().map(|c| c.name().to_owned()).collect();
            let target_columns: HashSet<String> = entity_report
                .renames
                .iter()
                .map(|r| r.from.clone())
                .collect();
            let computed_columns: HashSet<String> = entity_report
                .computed
                .iter()
                .map(|c| c.name.clone())
                .collect();

            entity_report.one_to_one = source_columns
                .difference(&target_columns)
                .filter(|col| !computed_columns.contains(*col))
                .cloned()
                .collect();
        }
    }

    fn sample_size(&self) -> usize {
        10 // TODO: make configurable
    }
}

#[async_trait]
impl DataProducer for ValidationProducer {
    async fn run(&mut self) -> Result<usize, ProducerError> {
        let results = self.perform_validation().await?;
        self.update_report(results).await;
        Ok(1) // One validation run completed
    }
}
