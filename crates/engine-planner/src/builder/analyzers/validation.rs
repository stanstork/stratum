use crate::{
    builder::{
        analysis::{AnalysisContext, AnalyzerError, AnalyzerResult, PlanAnalyzer},
        errors::ValidationAnalyzerError,
        utils::ColumnRefParser,
    },
    plan::validation::{
        plan::ValidationPlan,
        types::{ValidationAction, ValidationCheck, ValidationLevel},
    },
};
use async_trait::async_trait;
use connectors::{
    sql::base::{query::generator::QueryGenerator, requests::FetchRowsRequestBuilder},
    sql_filter_expr,
};
use engine_core::{
    connectors::linked::build_join_clauses,
    filter::{compiler::FilterCompiler, sql::SqlFilterCompiler},
};
use expression_engine::ExpressionAnalyzer;
use model::execution::{
    expr::CompiledExpression,
    pipeline::{
        Join, Pipeline, ValidationAction as ModelValidationAction, ValidationRule,
        ValidationSeverity,
    },
};
use tracing::{error, info, warn};

/// Analyzes validation rules to verify column availability and estimate failure probability.
pub struct ValidationAnalyzer;

impl ValidationAnalyzer {
    /// Primary orchestration logic for analyzing a single validation rule.
    async fn analyze_rule(
        &self,
        table: &str,
        validation: &ValidationRule,
        joins: &[Join],
        ctx: &AnalysisContext,
    ) -> AnalyzerResult<ValidationPlan> {
        // Extract and verify column references against the metadata graph
        let columns = ExpressionAnalyzer::extract_columns(&validation.check);
        self.verify_column_refs(&columns, ctx).map_err(|e| {
            AnalyzerError::error(
                "validation",
                format!("Column check failed for '{}': {}", validation.label, e),
            )
        })?;

        // Map severity levels and terminal actions
        let level = match validation.severity {
            ValidationSeverity::Assert => ValidationLevel::Assert,
            ValidationSeverity::Warn => ValidationLevel::Warn,
        };

        let action = self.validation_action(level.clone(), &validation.action);

        // Estimate failure probability using statistical sampling
        let estimated_failure_rate = self
            .estimate_probability(table, &validation.check, joins, ctx)
            .await
            .ok();

        info!(
            target: "analyzer",
            label = %validation.label,
            level = ?level,
            failure_rate = ?estimated_failure_rate,
            "Validation analysis complete"
        );

        Ok(ValidationPlan {
            name: validation.label.clone(),
            level,
            check: ValidationCheck {
                expression: ExpressionAnalyzer::to_string(&validation.check),
                columns_referenced: columns,
            },
            message: validation.message.clone(),
            action,
            estimated_failure_rate,
        })
    }

    /// Verifies that all columns in the validation exist within the source or joined tables.
    fn verify_column_refs(
        &self,
        columns: &[String],
        ctx: &AnalysisContext,
    ) -> Result<(), ValidationAnalyzerError> {
        let metadata_graph = ctx.schema_plan.metadata_graph();

        for col_ref in columns {
            let parsed = ColumnRefParser::parse(col_ref, "").map_err(|e| {
                ValidationAnalyzerError::ParseError(format!(
                    "Invalid reference '{}': {}",
                    col_ref, e
                ))
            })?;

            let exists = if !parsed.table.is_empty() {
                // Qualified: Resolve alias to physical table and check presence
                let physical_table = ctx.mapping.entities.reverse_resolve(&parsed.table);
                metadata_graph
                    .get(&physical_table)
                    .map(|meta| meta.columns().iter().any(|c| c.name() == parsed.column))
                    .unwrap_or(false)
            } else {
                // Unqualified: Scan all tables in the current metadata graph
                metadata_graph
                    .values()
                    .any(|meta| meta.columns().iter().any(|c| c.name() == parsed.column))
            };

            if !exists {
                return Err(ValidationAnalyzerError::ColumnNotFound {
                    column: col_ref.clone(),
                });
            }
        }
        Ok(())
    }

    /// Runs a sampling query to estimate how many rows might fail this validation.
    async fn estimate_probability(
        &self,
        table: &str,
        check: &CompiledExpression,
        joins: &[Join],
        ctx: &AnalysisContext,
    ) -> Result<f32, ValidationAnalyzerError> {
        // Identify tables involved to filter unnecessary joins
        let physical_tables: Vec<String> = ExpressionAnalyzer::extract_tables(check)
            .iter()
            .map(|t| ctx.mapping.entities.reverse_resolve(t))
            .collect();

        let related_joins: Vec<_> = joins
            .iter()
            .filter(|j| physical_tables.contains(&j.table))
            .cloned()
            .collect();

        let join_clauses = if !related_joins.is_empty() {
            build_join_clauses(&related_joins)
        } else {
            Vec::new()
        };

        // Compile logic into a physical SQL filter
        let sql_filter = SqlFilterCompiler::compile(check);
        let filter_expr = sql_filter
            .expr
            .as_ref()
            .and_then(|e| sql_filter_expr!(e).ok())
            .ok_or_else(|| {
                ValidationAnalyzerError::ParseError("Failed to compile check expression".into())
            })?;

        // Prepare request and generate estimation SQL using the correct source dialect
        let request = FetchRowsRequestBuilder::new(table.to_string())
            .joins(join_clauses)
            .build();
        let generator = QueryGenerator::new(ctx.source_dialect.as_ref());
        let (sql, params) = generator.validation_estimation(&request, filter_expr, 10_000);

        let rows = ctx
            .source_adapter
            .get_sql()
            .query_rows_params(&sql, params)
            .await
            .map_err(|e| {
                error!(target: "analyzer", error = %e, sql = %sql, "Probability estimation failed");
                ValidationAnalyzerError::QueryFailed(e.to_string())
            })?;

        if let Some(row) = rows.first() {
            let failures = row.get_value("failures").as_f64().unwrap_or(0.0) as f32;
            let total = row.get_value("total").as_i32().unwrap_or(0) as f32;

            if total > 0.0 {
                return Ok((failures / total).clamp(0.0, 1.0));
            }
        }

        Ok(0.0)
    }

    fn validation_action(
        &self,
        level: ValidationLevel,
        model_action: &ModelValidationAction,
    ) -> Option<ValidationAction> {
        if matches!(level, ValidationLevel::Assert) {
            Some(match model_action {
                ModelValidationAction::Fail => ValidationAction::Fail,
                _ => ValidationAction::Skip, // Assertions must at least skip the row
            })
        } else {
            Some(ValidationAction::Skip)
        }
    }
}

#[async_trait]
impl PlanAnalyzer for ValidationAnalyzer {
    type Input = Pipeline;
    type Output = Vec<ValidationPlan>;

    fn name(&self) -> &'static str {
        "validation"
    }

    async fn analyze(
        &self,
        pipeline: &Self::Input,
        ctx: &AnalysisContext,
    ) -> AnalyzerResult<Self::Output> {
        let table = &pipeline.source.table;
        let joins = &pipeline.source.joins;

        let mut plans = Vec::with_capacity(pipeline.validations.len());
        for validation in &pipeline.validations {
            match self.analyze_rule(table, validation, joins, ctx).await {
                Ok(plan) => plans.push(plan),
                Err(e) => {
                    warn!(target: "analyzer", label = %validation.label, error = %e, "Skipping rule due to error");
                    return Err(e);
                }
            }
        }

        info!(target: "analyzer", count = plans.len(), "Validation analysis completed");
        Ok(plans)
    }
}
