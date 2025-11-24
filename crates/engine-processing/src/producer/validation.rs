use crate::{error::ProducerError, producer::DataProducer, transform::pipeline::TransformPipeline};
use async_trait::async_trait;
use connectors::{
    metadata::entity::EntityMetadata,
    sql::base::{error::DbError, query::generator::QueryGenerator},
};
use engine_config::{
    report::{
        dry_run::{
            DryRunReport, DryRunStatus, FastPathCapabilities, FastPathSummary,
            OffsetValidationReport,
        },
        finding::{Finding, Severity},
        mapping::EntityMappingReport,
        sql::{SqlKind, SqlStatement},
        transform::{TransformationRecord, TransformationReport},
    },
    validation::schema_validator::DestinationSchemaValidator,
};
use engine_core::connectors::{
    destination::Destination,
    source::{DataSource, Source},
};
use futures::lock::Mutex;
use model::{pagination::cursor::Cursor, records::row::RowData, transform::mapping::EntityMapping};
use planner::query::offsets::OffsetStrategy;
use smql_syntax::ast::setting::{CopyColumns, Settings};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

/// A container for the results of the `sample_and_transform` operation.
#[derive(Debug, Clone)]
struct SampleResult {
    records_sampled: usize,
    reached_end: bool,
    next_cursor: Option<Cursor>,
    source_entity: Option<String>,
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
    report: Arc<Mutex<DryRunReport>>,
    source: Source,
    destination: Destination,
    pipeline: TransformPipeline,
    mapping: EntityMapping,
    settings: Settings,
    offset_strategy: Arc<dyn OffsetStrategy>,
    cursor: Cursor,
}

/// Bundles construction arguments for `ValidationProducer`.
pub struct ValidationProducerParams {
    pub source: Source,
    pub destination: Destination,
    pub pipeline: TransformPipeline,
    pub mapping: EntityMapping,
    pub settings: Settings,
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
        }
    }

    /// Initializes the destination schema validator.
    async fn init_schema_validator(&self) -> Result<DestinationSchemaValidator, ProducerError> {
        DestinationSchemaValidator::new(&self.destination, self.mapping.clone(), &self.settings)
            .await
            .map_err(|e| ProducerError::Other(format!("Init schema validator: {e}")))
    }

    /// Performs the core validation steps: SQL generation, sampling/transform, and final schema check.
    async fn perform_validation(&mut self) -> Result<ValidationResults, ProducerError> {
        let mut validator = self.init_schema_validator().await?;

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

    /// Updates the `DryRunReport` with the results of the validation.
    async fn update_report(&mut self, results: ValidationResults) {
        let mut report = self.report.lock().await;

        // Step 1: Update errors, sampled count, and SQL
        self.update_summary(&mut report, &results);
        // Step 2: Update transformation results
        self.update_transform(&mut report, &results.sample_result);
        // Step 3: Update mapping omissions and one-to-one columns
        self.update_mapping(&mut report, &results.sample_result)
            .await;
        // Step 4: Final validation results, offset, fast path, and status
        self.update_validation(&mut report, results).await;
    }

    /// Helper to update SQL statements, sampled counts, and all collected errors.
    fn update_summary(&self, report: &mut DryRunReport, results: &ValidationResults) {
        report
            .generated_sql
            .statements
            .extend(results.statements.clone());
        report.summary.records_sampled = results.sample_result.records_sampled;

        // Handle schema validation error
        if let Some(ref e) = results.schema_validation_error {
            let error_msg = format!("Schema validation error: {e}");
            report
                .summary
                .errors
                .push(Finding::new_fetch_error(&error_msg));
        }

        // Collect preparation, pruning, and sampling fetch errors
        report.summary.errors.extend(results.prep_findings.clone());
        report
            .summary
            .errors
            .extend(results.sample_result.prune_findings.clone());
        if let Some(ref err) = results.sample_result.fetch_error {
            report.summary.errors.push(err.clone());
        }
    }

    /// Helper to update the transformation report section.
    fn update_transform(&self, report: &mut DryRunReport, sample_result: &SampleResult) {
        if let Some(ref tr) = sample_result.transform_report {
            report.transform = tr.clone();
        }
    }

    /// Helper to update entity mapping findings (omitted columns) and one-to-one mapping.
    async fn update_mapping(&self, report: &mut DryRunReport, sample_result: &SampleResult) {
        for entity_report in &mut report.mapping.entities {
            // Update omitted columns from pruning step
            if let Some(omitted) = sample_result
                .omitted_columns
                .get(&entity_report.source_entity)
            {
                entity_report.omitted_source_columns.extend(omitted.clone());
            }

            // Update one-to-one mapped columns if CopyColumns::All is set
            if self.settings.copy_columns == CopyColumns::All {
                self.update_one_to_one_mapped(entity_report).await;
            }
        }
    }

    /// Helper to update final schema validation, offset, fast path summary, and final status.
    async fn update_validation(&self, report: &mut DryRunReport, results: ValidationResults) {
        report
            .schema_validation
            .findings
            .extend(results.schema_findings);
        report.offset_validation = self.offset_validation_report(&results.sample_result);
        report.fast_path_summary = self.fast_path_summary().await;
        report.summary.status = Self::calculate_status(report);
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

        if let Some(computed_fields) = self.mapping.field_mappings.get_computed(table)
            && computed_fields
                .iter()
                .any(|cf| cf.name.eq_ignore_ascii_case(field_name))
        {
            return Ok(true);
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

        let source_name = self.mapping.entity_name_map.reverse_resolve(table);
        if !dropped.is_empty() {
            omitted
                .entry(source_name)
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

        match self
            .source
            .fetch_data(self.sample_size(), self.cursor.clone())
            .await
        {
            Ok(data) => {
                let total = data.row_count;
                let next_cursor = data.next_cursor.clone();
                let reached_end = data.reached_end;
                let source_entity = data.rows.first().map(|r| r.entity.clone());
                let sample: Vec<TransformationRecord> = data
                    .rows
                    .into_iter()
                    .map(|input_row| {
                        let input_clone = input_row.clone();
                        let mut output_row = self.pipeline.apply(&input_row);

                        if output_row.entity.is_empty() {
                            output_row.entity = input_clone.entity.clone();
                        }
                        self.prune_row(&mut output_row, &mut prune_findings, &mut omitted_columns);
                        validator.validate(&output_row);

                        TransformationRecord {
                            input: input_clone,
                            output: Some(output_row),
                            error: None,
                            warnings: None,
                        }
                    })
                    .collect();

                let ok = sample.iter().filter(|r| r.output.is_some()).count();
                let failed = total.saturating_sub(ok);
                let report = TransformationReport { ok, failed, sample };

                SampleResult {
                    records_sampled: total,
                    reached_end,
                    next_cursor,
                    source_entity,
                    transform_report: Some(report),
                    fetch_error: None,
                    prune_findings,
                    omitted_columns,
                }
            }
            Err(e) => SampleResult {
                records_sampled: 0,
                reached_end: false,
                next_cursor: None,
                source_entity: None,
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

                let requests =
                    db.build_fetch_rows_requests(self.sample_size(), Cursor::Default { offset: 0 });
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

            entity_report.one_to_one.extend(
                source_columns
                    .difference(&target_columns)
                    .filter(|col| !computed_columns.contains(*col))
                    .cloned()
                    .collect::<Vec<_>>(),
            );
        }
    }

    fn sample_size(&self) -> usize {
        10 // TODO: make configurable
    }

    fn offset_validation_report(&self, sample: &SampleResult) -> OffsetValidationReport {
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

    async fn fast_path_summary(&self) -> FastPathSummary {
        let sink = self.destination.sink();
        let adapter = self.destination.data_dest.adapter().await;

        let (capabilities, capability_probe_error) = match adapter.capabilities().await {
            Ok(caps) => (
                Some(FastPathCapabilities {
                    copy_streaming: caps.copy_streaming,
                    merge_statements: caps.merge_statements,
                }),
                None,
            ),
            Err(e) => (
                None,
                Some(format!("Fast path capability probe failed: {e}")),
            ),
        };

        match sink.support_fast_path().await {
            Ok(true) => match adapter.table_exists(&self.destination.name).await {
                Ok(true) => match self
                    .destination
                    .data_dest
                    .fetch_meta(self.destination.name.clone())
                    .await
                {
                    Ok(meta) => {
                        if meta.primary_keys.is_empty() {
                            FastPathSummary {
                                supported: false,
                                reason: Some(
                                    "Fast path disabled: destination table has no primary key"
                                        .to_string(),
                                ),
                                capabilities,
                            }
                        } else {
                            FastPathSummary {
                                supported: true,
                                reason: None,
                                capabilities,
                            }
                        }
                    }
                    Err(e) => FastPathSummary {
                        supported: false,
                        reason: Some(format!("Failed to fetch destination metadata: {e}")),
                        capabilities,
                    },
                },
                Ok(false) if self.settings.create_missing_tables => {
                    let source_table = self
                        .mapping
                        .entity_name_map
                        .reverse_resolve(&self.destination.name);
                    match self.source.primary.fetch_meta(source_table.clone()).await {
                        Ok(EntityMetadata::Table(meta)) => {
                            if meta.primary_keys.is_empty() {
                                FastPathSummary {
                                    supported: false,
                                    reason: Some(format!(
                                        "Fast path disabled: source table `{source_table}` has no primary key"
                                    )),
                                    capabilities,
                                }
                            } else {
                                FastPathSummary {
                                    supported: true,
                                    reason: None,
                                    capabilities,
                                }
                            }
                        }
                        Ok(_) => FastPathSummary {
                            supported: false,
                            reason: Some(format!(
                                "Fast path disabled: cannot infer primary keys for `{source_table}`"
                            )),
                            capabilities,
                        },
                        Err(e) => FastPathSummary {
                            supported: false,
                            reason: Some(format!(
                                "Failed to fetch source metadata for `{source_table}`: {e}"
                            )),
                            capabilities,
                        },
                    }
                }
                Ok(false) => FastPathSummary {
                    supported: false,
                    reason: Some(
                        "Destination table does not exist and auto-creation is disabled"
                            .to_string(),
                    ),
                    capabilities,
                },
                Err(e) => FastPathSummary {
                    supported: false,
                    reason: Some(format!("Fast path table existence check failed: {e}")),
                    capabilities,
                },
            },
            Ok(false) => FastPathSummary {
                supported: false,
                reason: Some("Destination sink does not support fast path".to_string()),
                capabilities,
            },
            Err(e) => FastPathSummary {
                supported: false,
                reason: Some(
                    capability_probe_error
                        .unwrap_or_else(|| format!("Fast path check failed: {e}")),
                ),
                capabilities,
            },
        }
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
