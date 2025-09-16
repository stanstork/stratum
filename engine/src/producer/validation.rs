use crate::{
    destination::Destination,
    producer::{schema_validator::DestinationSchemaValidator, DataProducer},
    report::{
        dry_run::DryRunStatus,
        finding::{FetchFinding, Finding, MappingFinding, Severity},
        sql::{SqlKind, SqlStatement},
        transform::{TransformationRecord, TransformationReport},
    },
    source::{data::DataSource, Source},
    state::MigrationState,
    transform::pipeline::TransformPipeline,
};
use async_trait::async_trait;
use common::{mapping::EntityMapping, row_data::RowData};
use query_builder::dialect::{self, Dialect};
use smql::statements::setting::{CopyColumns, Settings};
use sql_adapter::query::generator::QueryGenerator;
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

/// The producer for a validation run. Fetches a small sample, transforms it,
/// and writes the results and diagnostics to the ValidationReport.
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

    fn is_allowed_output_field(&self, table: &str, field_name: &str) -> Result<bool, Finding> {
        if self.settings.copy_columns == CopyColumns::All {
            return Ok(true);
        }

        let colmap = self
            .mapping
            .field_mappings
            .column_mappings
            .get(table)
            .ok_or_else(|| MappingFinding::create_missing_finding(table, ""))?;

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

    /// Removes fields from a `RowData` instance that are not part of the defined mapping
    /// when `mapped_columns_only` is true.
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
            findings.push(MappingFinding::create_missing_finding(
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

    /// Fetches a sample of data, applies the transformation pipeline, and prunes unmapped columns.
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
                fetch_error: Some(FetchFinding::create_error_finding(&e.to_string())),
                prune_findings,
                omitted_columns,
            },
        }
    }

    /// Generates SQL statements for fetching data from a database source.
    async fn generate_sql_statements(&self) -> (Vec<SqlStatement>, Vec<Finding>) {
        match &self.source.primary {
            DataSource::Database(db_arc) => {
                let db = db_arc.lock().await;
                let dialect_impl = dialect::MySql; // TODO: Determine dialect from source
                let generator = QueryGenerator::new(&dialect_impl);

                let requests = db.build_fetch_rows_requests(self.sample_size(), None);
                let statements = requests
                    .into_iter()
                    .map(|req| {
                        let (sql, params) = generator.select(&req);
                        SqlStatement {
                            dialect: dialect_impl.name(),
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

    fn sample_size(&self) -> usize {
        10 // TODO: make configurable
    }
}

#[async_trait]
impl DataProducer for ValidationProducer {
    async fn run(&mut self) -> usize {
        let mut validator = DestinationSchemaValidator::new(
            &self.destination,
            self.mapping.clone(),
            &self.settings,
        )
        .await
        .expect("Failed to initialize schema validator");

        let (statements, prep_findings) = self.generate_sql_statements().await;
        let sample_result = self.sample_and_transform(&mut validator).await;

        let state = self.state.lock().await;
        let mut report = state.dry_run_report.lock().await;

        report.generated_sql.statements.extend(statements);
        report.summary.records_sampled = sample_result.records_sampled;

        if let Some(tr) = sample_result.transform_report {
            report.transform = tr;
        }

        report.summary.errors.extend(prep_findings);
        report.summary.errors.extend(sample_result.prune_findings);
        if let Some(err) = sample_result.fetch_error {
            report.summary.errors.push(err);
        }

        for entity_report in &mut report.mapping.entities {
            if let Some(omitted) = sample_result
                .omitted_columns
                .get(&entity_report.source_entity)
            {
                entity_report.omitted_source_columns.extend(omitted.clone());
            }
        }

        report
            .schema_validation
            .findings
            .extend(validator.findings());

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
                .any(|r| r.warnings.as_ref().map_or(false, |w| !w.is_empty()));

        report.summary.status = if has_errors {
            DryRunStatus::Failure
        } else if has_warnings {
            DryRunStatus::SuccessWithWarnings
        } else {
            DryRunStatus::Success
        };

        sample_result.records_sampled
    }
}
