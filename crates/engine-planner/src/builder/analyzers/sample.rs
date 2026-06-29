use crate::{
    builder::{
        analysis::{AnalysisContext, AnalyzerError, AnalyzerResult, PlanAnalyzer},
        errors::SampleCollectorError,
        utils::MaskingPolicy,
    },
    plan::{
        sample::{
            issue::{SampleIssue, SampleIssueLevel},
            method::SamplingMethod,
            preview::{SampleDataPreview, SampleQuery},
            row::{SampleRow, SampleRowStatus, SampleValidationResult, SampleValue},
            stats::{SampleStats, ValidationStats},
        },
        validation::plan::ValidationPlan,
    },
};
use async_trait::async_trait;
use connectors::sql::{query::generator::QueryGenerator, request::FetchRowsRequestBuilder};
use engine_processing::io::driver::SchemaDriver;
use engine_processing::{
    EnvContext,
    producer::build_transform_pipeline,
    transform::{
        error::TransformError,
        pipeline::{ApplyOutcome, TransformPipeline, ValidationWarning},
    },
};
use model::{
    core::value::Value, execution::pipeline::Pipeline, records::Record,
    transform::mapping::TransformationMetadata,
};
use std::{collections::HashMap, fmt::Write, sync::Arc, time::Instant};
use tracing::info;

/// Configuration for sample collection behavior and privacy
#[derive(Clone, Debug)]
pub struct SampleConfig {
    /// Whether sampling is enabled
    pub enabled: bool,
    pub size: usize,
    pub method: SamplingMethod,
    pub mask_columns: Vec<String>,
    pub auto_mask_sensitive: bool,
    pub sample_ids: Option<Vec<Value>>,
    pub id_column: String,
}

impl Default for SampleConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            size: 5,
            method: SamplingMethod::First,
            mask_columns: Vec::new(),
            auto_mask_sensitive: true,
            sample_ids: None,
            id_column: "id".to_string(),
        }
    }
}

/// Orchestrates the collection, transformation, and validation of data samples
/// to provide a "dry-run" preview of the pipeline's behavior.
pub struct SampleCollector<S: SchemaDriver> {
    src_driver: Arc<S>,
    config: SampleConfig,
}

struct ValidationContext<'a> {
    validations: &'a [ValidationPlan],
    mapping: &'a TransformationMetadata,
    val_stats: &'a mut HashMap<String, (usize, usize)>,
    results: &'a mut Vec<SampleValidationResult>,
}

impl<S: SchemaDriver> SampleCollector<S> {
    pub fn new(src_driver: Arc<S>, config: SampleConfig) -> Self {
        Self { src_driver, config }
    }

    pub async fn collect<D: SchemaDriver>(
        &self,
        pipeline: &Pipeline,
        mapping: &TransformationMetadata,
        validations: &[ValidationPlan],
        mapped_columns_only: bool,
        masking: &MaskingPolicy,
        ctx: &AnalysisContext<S, D>,
    ) -> Result<SampleDataPreview, SampleCollectorError> {
        let start = Instant::now();

        let (mut source_rows, query) = self.fetch_sample(pipeline, masking, ctx).await?;
        if source_rows.is_empty() {
            return Ok(self.empty_preview(start, query));
        }

        source_rows
            .iter_mut()
            .for_each(|r| r.schema = pipeline.source.table.clone());

        let transform_pipeline = build_transform_pipeline(
            pipeline,
            &ctx.plugin_registry,
            mapping,
            mapped_columns_only,
            Arc::new(EnvContext::empty()),
        )
        .map_err(|e| SampleCollectorError::PipelineBuildFailed(e.to_string()))?;
        let mut sample_rows = Vec::with_capacity(source_rows.len());
        let mut val_stats: HashMap<String, (usize, usize)> = HashMap::new();

        for (idx, mut row) in source_rows.into_iter().enumerate() {
            sample_rows.push(self.process_sample_row(
                idx,
                &mut row,
                &transform_pipeline,
                validations,
                mapping,
                &mut val_stats,
            ));
        }

        info!(table = %pipeline.source.table, count = sample_rows.len(), "collected sample rows");

        Ok(SampleDataPreview {
            enabled: true,
            sampled_at: Some(chrono::Utc::now()),
            sample_size: sample_rows.len(),
            sampling_method: self.config.method.clone(),
            duration_ms: Some(start.elapsed().as_millis() as u64),
            query,
            stats: self.aggregate_stats(&sample_rows, &val_stats),
            issues: sample_rows
                .iter()
                .flat_map(|r| r.issues.iter().cloned())
                .collect(),
            rows: sample_rows,
        })
    }

    /// Handles the transformation and validation lifecycle for a single row.
    fn process_sample_row(
        &self,
        idx: usize,
        row: &mut Record,
        pipeline: &TransformPipeline,
        validations: &[ValidationPlan],
        mapping: &TransformationMetadata,
        val_stats: &mut HashMap<String, (usize, usize)>,
    ) -> SampleRow {
        let input_values = self.map_to_sample_values(row);
        let source_id = self.extract_identifier(row);

        let mut status = SampleRowStatus::Ok;
        let mut issues = Vec::new();
        let mut output = None;
        let mut validation_results = Vec::new();

        match pipeline.apply(row) {
            Ok(ApplyOutcome::Success) => {
                output = Some(self.map_to_sample_values(row));
                self.record_passed(validations, val_stats, &mut validation_results);
            }
            Ok(ApplyOutcome::Warning { warnings }) => {
                output = Some(self.map_to_sample_values(row));
                status = SampleRowStatus::Warning;
                self.handle_validation_warnings(
                    idx,
                    row,
                    &warnings,
                    &mut ValidationContext {
                        validations,
                        mapping,
                        val_stats,
                        results: &mut validation_results,
                    },
                    &mut issues,
                );
            }
            Ok(ApplyOutcome::Skipped { reason }) => {
                status = SampleRowStatus::Skipped;
                issues.push(self.info_issue(
                    idx,
                    "SKIPPED",
                    &reason.unwrap_or_else(|| "Filtered by logic".into()),
                ));
            }
            Err(e) => {
                let (err_status, issue) = self.handle_transform_error(
                    idx,
                    e,
                    row,
                    &mut ValidationContext {
                        validations,
                        mapping,
                        val_stats,
                        results: &mut validation_results,
                    },
                );
                status = err_status;
                issues.push(issue);
            }
        }

        SampleRow {
            index: idx,
            source_id,
            input: input_values,
            output,
            status,
            validations: validation_results,
            issues,
        }
    }

    fn record_passed(
        &self,
        validations: &[ValidationPlan],
        val_stats: &mut HashMap<String, (usize, usize)>,
        results: &mut Vec<SampleValidationResult>,
    ) {
        for v in validations {
            val_stats.entry(v.name.clone()).or_insert((0, 0)).0 += 1;
            results.push(SampleValidationResult {
                name: v.name.clone(),
                passed: true,
                check: v.check.expression.clone(),
                actual_values: String::new(),
                message: None,
            });
        }
    }

    fn handle_validation_warnings(
        &self,
        idx: usize,
        row: &Record,
        warnings: &[ValidationWarning],
        val_ctx: &mut ValidationContext,
        issues: &mut Vec<SampleIssue>,
    ) {
        let failed_names: std::collections::HashSet<_> =
            warnings.iter().map(|w| w.rule.clone()).collect();

        for warning in warnings {
            val_ctx
                .val_stats
                .entry(warning.rule.clone())
                .or_insert((0, 0))
                .1 += 1;

            if let Some(v) = val_ctx
                .validations
                .iter()
                .find(|val| val.name == warning.rule)
            {
                val_ctx.results.push(SampleValidationResult {
                    name: v.name.clone(),
                    passed: false,
                    check: v.check.expression.clone(),
                    actual_values: self.format_val_context(
                        row,
                        &v.check.columns_referenced,
                        val_ctx.mapping,
                    ),
                    message: Some(warning.message.clone()),
                });
            }

            issues.push(SampleIssue {
                level: SampleIssueLevel::Warning,
                code: "VALIDATION_WARNING".into(),
                message: format!("Warning for '{}': {}", warning.rule, warning.message),
                row_index: Some(idx),
                column: None,
                suggestion: Some("Verify input data constraints".into()),
            });
        }

        for v in val_ctx
            .validations
            .iter()
            .filter(|v| !failed_names.contains(&v.name))
        {
            val_ctx.val_stats.entry(v.name.clone()).or_insert((0, 0)).0 += 1;
            val_ctx.results.push(SampleValidationResult {
                name: v.name.clone(),
                passed: true,
                check: v.check.expression.clone(),
                actual_values: String::new(),
                message: None,
            });
        }
    }

    fn handle_transform_error(
        &self,
        idx: usize,
        err: TransformError,
        row: &Record,
        val_ctx: &mut ValidationContext,
    ) -> (SampleRowStatus, SampleIssue) {
        match err {
            TransformError::ValidationFailed { rule, message } => {
                val_ctx.val_stats.entry(rule.clone()).or_insert((0, 0)).1 += 1;
                if let Some(v) = val_ctx.validations.iter().find(|val| val.name == rule) {
                    val_ctx.results.push(SampleValidationResult {
                        name: v.name.clone(),
                        passed: false,
                        check: v.check.expression.clone(),
                        actual_values: self.format_val_context(
                            row,
                            &v.check.columns_referenced,
                            val_ctx.mapping,
                        ),
                        message: Some(message.clone()),
                    });
                }
                (
                    SampleRowStatus::Failed,
                    SampleIssue {
                        level: SampleIssueLevel::Failed,
                        code: "VALIDATION_FAILED".into(),
                        message: format!("Validation '{}' failed: {}", rule, message),
                        row_index: Some(idx),
                        column: None,
                        suggestion: Some("Review constraints".into()),
                    },
                )
            }
            TransformError::FilteredOut => (
                SampleRowStatus::Skipped,
                self.info_issue(idx, "FILTERED", "Row filtered"),
            ),
            _ => (
                SampleRowStatus::Failed,
                SampleIssue {
                    level: SampleIssueLevel::Failed,
                    code: "TRANSFORM_ERROR".into(),
                    message: format!("Error: {}", err),
                    row_index: Some(idx),
                    column: None,
                    suggestion: Some("Check mapping logic and expressions".into()),
                },
            ),
        }
    }

    async fn fetch_sample<D: SchemaDriver>(
        &self,
        pipeline: &Pipeline,
        masking: &MaskingPolicy,
        ctx: &AnalysisContext<S, D>,
    ) -> Result<(Vec<Record>, Option<SampleQuery>), SampleCollectorError> {
        let mut request = FetchRowsRequestBuilder::new(pipeline.source.table.clone())
            .limit(self.config.size)
            .build();

        match self.config.method {
            SamplingMethod::Random => request.order_random = true,
            SamplingMethod::ById => {
                let ids = self.config.sample_ids.as_ref().ok_or(
                    SampleCollectorError::MissingRequiredConfig {
                        field: "sample_ids".into(),
                        method: "ById".into(),
                    },
                )?;
                request.in_clause = Some((self.config.id_column.clone(), ids.clone()));
            }
            SamplingMethod::Stratified => {
                return Err(SampleCollectorError::UnsupportedSamplingMethod {
                    method: "Stratified".into(),
                });
            }
            SamplingMethod::First => {}
        }

        let dialect = ctx.source_dialect.as_query_dialect();
        let generator = QueryGenerator::new(dialect.as_ref());
        let (sql, params) = generator.select(&request);
        let query = Some(SampleQuery {
            sql: sql.clone(),
            params: self.format_query_params(&params, masking),
        });

        let rows = self
            .src_driver
            .query_params(&sql, &params)
            .await
            .map_err(|e| SampleCollectorError::QueryExecutionFailed {
                table: pipeline.source.table.clone(),
                error: e.to_string(),
            })?;

        Ok((rows, query))
    }

    /// Resolves column values from the current row, accounting for table aliases in joins.
    fn resolve_val(
        &self,
        row: &Record,
        col_ref: &str,
        mapping: &TransformationMetadata,
    ) -> Option<Value> {
        if let Some(f) = row.get(col_ref) {
            return f.value.clone();
        }

        if let Some((alias, field)) = col_ref.split_once('.') {
            for refs in mapping.foreign_fields.values() {
                for cr in refs {
                    if cr.entity.eq_ignore_ascii_case(alias)
                        && cr.field.eq_ignore_ascii_case(field)
                        && let Some(target) = &cr.target
                        && let Some(f) = row.get(target)
                    {
                        return f.value.clone();
                    }
                }
            }
            if let Some(f) = row.get(field) {
                return f.value.clone();
            }
        }

        row.get(col_ref.split('.').next_back()?)
            .and_then(|f| f.value.clone())
    }

    fn format_val_context(
        &self,
        row: &Record,
        cols: &[String],
        mapping: &TransformationMetadata,
    ) -> String {
        if cols.is_empty() {
            return "<no columns referenced>".into();
        }
        let mut buf = String::new();
        for (i, col) in cols.iter().enumerate() {
            if i > 0 {
                buf.push_str(", ");
            }
            let val = self.resolve_val(row, col, mapping);
            let val_as_string = val
                .as_ref()
                .and_then(|v| v.as_string())
                .unwrap_or_else(|| "NULL".into());
            let _ = write!(buf, "{}={}", col, val_as_string);
        }
        buf
    }

    fn map_to_sample_values(&self, row: &Record) -> HashMap<String, SampleValue> {
        const MAX_DISPLAY: usize = 120;
        row.fields
            .iter()
            .map(|f| {
                let (display, is_null, truncated, len) = match &f.value {
                    Some(v) => {
                        let s = v.as_string().unwrap_or_else(|| format!("{:?}", v));
                        if s.len() > MAX_DISPLAY {
                            (
                                format!("{}...", &s[..MAX_DISPLAY]),
                                false,
                                true,
                                Some(s.len()),
                            )
                        } else {
                            (s, false, false, None)
                        }
                    }
                    None => ("NULL".into(), true, false, None),
                };
                (
                    f.name.clone(),
                    SampleValue {
                        display,
                        value_type: format!("{:?}", f.data_type),
                        is_null,
                        truncated,
                        original_length: len,
                    },
                )
            })
            .collect()
    }

    fn aggregate_stats(
        &self,
        rows: &[SampleRow],
        val_results: &HashMap<String, (usize, usize)>,
    ) -> SampleStats {
        let mut stats = SampleStats {
            ok: rows
                .iter()
                .filter(|r| r.status == SampleRowStatus::Ok)
                .count(),
            warnings: rows
                .iter()
                .filter(|r| r.status == SampleRowStatus::Warning)
                .count(),
            skipped: rows
                .iter()
                .filter(|r| r.status == SampleRowStatus::Skipped)
                .count(),
            errors: rows
                .iter()
                .filter(|r| r.status == SampleRowStatus::Failed)
                .count(),
            validation_stats: Vec::new(),
        };

        for (name, (passed, failed)) in val_results {
            let total = passed + failed;
            stats.validation_stats.push(ValidationStats {
                name: name.clone(),
                passed: *passed,
                failed: *failed,
                pass_rate: if total > 0 {
                    *passed as f32 / total as f32
                } else {
                    0.0
                },
            });
        }
        stats
    }

    fn extract_identifier(&self, row: &Record) -> Option<String> {
        ["id", "_id", "uuid", "pk", &self.config.id_column]
            .iter()
            .find_map(|&c| row.fields.iter().find(|f| f.name.eq_ignore_ascii_case(c)))
            .and_then(|f| f.value.as_ref().map(|v| format!("{:?}", v)))
    }

    fn empty_preview(&self, start: Instant, query: Option<SampleQuery>) -> SampleDataPreview {
        SampleDataPreview {
            enabled: true,
            sampled_at: Some(chrono::Utc::now()),
            sample_size: 0,
            sampling_method: self.config.method.clone(),
            duration_ms: Some(start.elapsed().as_millis() as u64),
            query,
            rows: Vec::new(),
            stats: SampleStats::default(),
            issues: vec![self.info_issue(0, "EMPTY", "No source data found")],
        }
    }

    fn info_issue(&self, idx: usize, code: &str, msg: &str) -> SampleIssue {
        SampleIssue {
            level: SampleIssueLevel::Info,
            code: code.into(),
            message: msg.into(),
            row_index: Some(idx),
            column: None,
            suggestion: None,
        }
    }

    fn apply_masking(&self, preview: &mut SampleDataPreview, masking: &MaskingPolicy) {
        for row in &mut preview.rows {
            for val in row.input.values_mut() {
                if masking.should_mask(&val.display) && !val.is_null {
                    val.display = masking.mask_value(&val.display);
                }
            }
            if let Some(out) = &mut row.output {
                for val in out.values_mut() {
                    if masking.should_mask(&val.display) && !val.is_null {
                        val.display = masking.mask_value(&val.display);
                    }
                }
            }
        }
    }

    fn format_query_params(&self, params: &[Value], masking: &MaskingPolicy) -> Vec<String> {
        params
            .iter()
            .map(|value| self.format_query_param(value, masking))
            .collect()
    }

    fn format_query_param(&self, value: &Value, masking: &MaskingPolicy) -> String {
        let raw = value.as_string().unwrap_or_else(|| format!("{:?}", value));

        if MaskingPolicy::is_db_url(&raw) {
            return MaskingPolicy::mask_url(&raw);
        }

        if !masking.auto_mask_sensitive {
            return raw;
        }

        // Mask string-like values that might contain sensitive data
        match value {
            Value::String(_) | Value::Null => masking.mask_value(&raw),
            _ => raw,
        }
    }
}

#[async_trait]
impl<S: SchemaDriver, D: SchemaDriver> PlanAnalyzer<S, D> for SampleCollector<S> {
    type Input = (Pipeline, TransformationMetadata, Vec<ValidationPlan>, bool);
    type Output = SampleDataPreview;

    fn name(&self) -> &'static str {
        "sample"
    }

    async fn analyze(
        &self,
        input: &Self::Input,
        ctx: &AnalysisContext<S, D>,
    ) -> AnalyzerResult<Self::Output> {
        let (pipeline, mapping, validations, mapped_columns_only) = input;

        let mut preview = self
            .collect(
                pipeline,
                mapping,
                validations,
                *mapped_columns_only,
                &ctx.masking,
                ctx,
            )
            .await
            .map_err(|e| AnalyzerError::error("sample", e.to_string()))?;

        self.apply_masking(&mut preview, &ctx.masking);
        Ok(preview)
    }
}
