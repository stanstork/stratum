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
use common::{mapping::EntityMapping, row_data::RowData};
use query_builder::dialect::{self, Dialect};
use smql::statements::setting::CopyColumns;
use sql_adapter::query::generator::QueryGenerator;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tokio::sync::Mutex;

/// The producer for a validation run. Fetches a small sample, transforms it,
/// and writes the results and diagnostics to the ValidationReport.
pub struct ValidationProducer {
    state: Arc<Mutex<MigrationState>>,
    source: Source,
    pipeline: TransformPipeline,
    mapping: EntityMapping,
    sample_size: usize,
    mapped_columns_only: bool,
}

impl ValidationProducer {
    pub fn new(
        state: Arc<Mutex<MigrationState>>,
        source: Source,
        pipeline: TransformPipeline,
        mapping: EntityMapping,
        sample_size: usize,
        mapped_columns_only: bool,
    ) -> Self {
        Self {
            state,
            source,
            mapping,
            pipeline,
            sample_size,
            mapped_columns_only,
        }
    }

    /// Return true if a field should be kept in the transformed output for `table`.
    /// Keeps any field that is a mapped *target* column or a computed field.
    fn is_allowed_output_field(&self, table: &str, field_name: &str) -> Result<bool, Finding> {
        if !self.mapped_columns_only {
            return Ok(true);
        }

        // Column mappings for this table must exist when pruning is enabled.
        let Some(colmap) = self.mapping.field_mappings.column_mappings.get(table) else {
            return Err(Finding {
                code: "MAPPING_MISSING".into(),
                message: format!(
                    "No mapping found for table `{table}` while `mapped_columns_only` is set."
                ),
                severity: Severity::Error,
                kind: FindingKind::SourceSchema,
                location: None,
                suggestion: Some(
                    "Add field mappings for this table or disable `mapped_columns_only`.".into(),
                ),
            });
        };

        // Allow if it's a mapped TARGET column
        if colmap.contains_target_key(field_name) {
            return Ok(true);
        }

        // Or if it's a computed field name for this table
        if let Some(computed) = self.mapping.field_mappings.get_computed(table) {
            if computed
                .iter()
                .any(|cf| cf.name.eq_ignore_ascii_case(field_name))
            {
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Prune `RowData.field_values` in-place according to mapping rules for its entity (table).
    fn prune_row(
        &self,
        row: &mut RowData,
        findings: &mut Vec<Finding>,
        omitted: &mut HashMap<String, HashSet<String>>,
    ) {
        if !self.mapped_columns_only {
            return;
        }

        let table = row.entity.as_str();

        // If mapping missing, record a finding and drop everything (no mapped targets / computed).
        let has_mapping = self
            .mapping
            .field_mappings
            .column_mappings
            .get(table)
            .is_some();
        if !has_mapping {
            findings.push(Finding {
                code: "MAPPING_MISSING".into(),
                message: format!(
                    "No mapping found for table `{table}` while `mapped_columns_only` is set. Output row will be empty."
                ),
                severity: Severity::Error,
                kind: FindingKind::SourceSchema,
                location: None,
                suggestion: Some("Add field mappings for this table.".into()),
            });
            omitted
                .entry(table.to_string())
                .or_default()
                .extend(row.field_values.iter().map(|fv| fv.name.clone()));
            row.field_values.clear();
            return;
        }

        let mut retained = Vec::with_capacity(row.field_values.len());
        let mut dropped = Vec::new();

        for fv in row.field_values.drain(..) {
            match self.is_allowed_output_field(table, &fv.name) {
                Ok(true) => retained.push(fv),
                Ok(false) => dropped.push(fv.name),
                Err(f) => {
                    findings.push(f);
                    dropped.push(fv.name);
                }
            }
        }

        if !dropped.is_empty() {
            omitted
                .entry(table.to_string())
                .or_default()
                .extend(dropped);
        }

        row.field_values = retained;
    }

    /// Execute a sample fetch and run transformations, pruning unmapped fields afterward.
    async fn sample_and_transform(
        &self,
    ) -> (
        usize,
        Option<TransformationReport>,
        Option<Finding>,
        Vec<Finding>,
        HashMap<String, HashSet<String>>, // omitted columns by table
    ) {
        let mut prune_findings = Vec::new();
        let mut omitted_columns: HashMap<String, HashSet<String>> = HashMap::new();

        match self.source.fetch_data(self.sample_size, None).await {
            Ok(data) => {
                let mut sample: Vec<TransformationRecord> = Vec::with_capacity(data.len());

                for record in &data {
                    if let Some(input_row) = record.to_row_data().cloned() {
                        // Apply pipeline
                        let transformed = self.pipeline.apply(record);

                        // Convert to RowData (may be None if pipeline filters/errs)
                        let mut output_row_opt = transformed.to_row_data().cloned();

                        // If we have output, prune unmapped columns by table
                        if let Some(ref mut output_row) = output_row_opt {
                            // Use the entity name from the OUTPUT row; if empty, fall back to input.
                            if output_row.entity.is_empty() {
                                output_row.entity = input_row.entity.clone();
                            }
                            self.prune_row(output_row, &mut prune_findings, &mut omitted_columns);
                        }

                        sample.push(TransformationRecord {
                            input: input_row,
                            output: output_row_opt,
                            error: None,
                            warnings: None,
                        });
                    }
                }

                let ok = sample.iter().filter(|r| r.output.is_some()).count();
                let total = data.len();
                let failed = total.saturating_sub(ok);

                (
                    total,
                    Some(TransformationReport { ok, failed, sample }),
                    None,
                    prune_findings,
                    omitted_columns,
                )
            }
            Err(e) => {
                let finding = Finding {
                    code: "FETCH_ERROR".into(),
                    message: format!("Error fetching data: {e}"),
                    severity: Severity::Error,
                    kind: FindingKind::SourceData,
                    location: None,
                    suggestion: Some("Check source connectivity and query validity.".into()),
                };
                (0, None, Some(finding), prune_findings, omitted_columns)
            }
        }
    }
}

#[async_trait]
impl DataProducer for ValidationProducer {
    async fn run(&mut self) -> usize {
        // TODO: Determine dialect from connection; hard-coded for now.
        let dialect_impl = dialect::MySql;

        let (statements, prep_findings): (Vec<SqlStatement>, Vec<Finding>) =
            match &self.source.primary {
                DataSource::Database(db_arc) => {
                    let db = db_arc.lock().await;
                    let reqs = db.build_fetch_rows_requests(self.sample_size, None);
                    let generator = QueryGenerator::new(&dialect_impl);

                    let statements = reqs
                        .into_iter()
                        .map(|r| {
                            let (sql, params) = generator.select(&r);
                            SqlStatement {
                                dialect: dialect_impl.name(),
                                kind: SqlKind::Data,
                                sql,
                                params,
                            }
                        })
                        .collect::<Vec<_>>();

                    (statements, Vec::new())
                }
                _ => (
                    Vec::new(),
                    vec![Finding {
                        code: "UNSUPPORTED_SOURCE".into(),
                        message: format!(
                            "Validation run does not support source type: {:?}",
                            self.source.format()
                        ),
                        severity: Severity::Error,
                        kind: FindingKind::SourceSchema,
                        location: None,
                        suggestion: Some("Use a database source for validation runs.".into()),
                    }],
                ),
            };

        // Fetch & transform, then prune unmapped fields from transformed RowData
        let (records_sampled, transform_report, fetch_error, prune_findings, omitted_columns) =
            self.sample_and_transform().await;

        // Commit to report
        let state = self.state.lock().await;
        let mut report = state.dry_run_report.lock().await;

        report.generated_sql.statements.extend(statements);
        report.summary.records_sampled = records_sampled;

        if let Some(tr) = transform_report {
            report.transform = tr;
        }

        report.summary.errors.extend(prep_findings);
        report.summary.errors.extend(prune_findings);
        if let Some(err) = fetch_error {
            report.summary.errors.push(err);
        }

        // record omitted columns
        for r in &mut report.mapping.entities {
            if let Some(omitted) = omitted_columns.get(&r.source_entity) {
                r.omitted_source_columns.extend(omitted.clone());
            }
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
