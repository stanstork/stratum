use crate::{
    builder::{
        analysis::{AnalysisContext, AnalyzerError, AnalyzerResult, PlanAnalyzer},
        errors::JoinAnalyzerError,
    },
    plan::transform::join::{JoinColumn, JoinCondition, JoinPlan, JoinType},
};
use async_trait::async_trait;
use connectors::join_on_expr;
use connectors::sql::base::join::clause::{JoinClause, JoinCondition as CoreJoinCondition};
use engine_core::connectors::linked::build_join_clauses;
use model::execution::pipeline::Join;
use query_builder::renderer::{Render, Renderer};
use tracing::info;

/// Analyzes join operations to determine performance risks and optimization opportunities.
pub struct JoinAnalyzer;

impl JoinAnalyzer {
    /// Orchestrates the analysis of a single join within a pipeline.
    pub async fn analyze_single_join(
        &self,
        join: &Join,
        ctx: &AnalysisContext,
    ) -> Result<JoinPlan, JoinAnalyzerError> {
        let table = &join.table;
        let alias = join.alias.clone();

        // Volume Estimation: Fetch row counts from the shared context cache.
        let table_rows = ctx.source_cache.count_rows(table, None).await;

        // Clause Construction: Build the physical join clauses.
        let join_clauses = build_join_clauses(std::slice::from_ref(join));
        let join_clause = join_clauses.first().ok_or_else(|| {
            AnalyzerError::error(
                "join",
                format!("Failed to construct join clause for table: {}", table),
            )
        })?;

        // Performance Analysis: Check index coverage and estimate data match rate.
        let indexed = self.verify_index_coverage(table, join_clause, ctx).await;
        let match_rate = self.estimate_match_rate(&join_clause.conditions).await;

        // SQL Representation: Render the join condition for the final plan view.
        let sql = self.render_join_sql(join_clause, ctx);

        // Diagnostic Generation: Identify potential performance bottlenecks.
        let warnings = self.performance_warnings(table, join_clause, indexed);

        info!(
            target: "analyzer",
            table = %table,
            indexed = %indexed,
            match_rate = ?match_rate,
            "Join analysis complete"
        );

        Ok(JoinPlan {
            alias,
            source_table: table.clone(),
            join_type: JoinType::Inner,
            conditions: self.map_conditions(join, join_clause, &sql, indexed),
            columns_used: Vec::new(), // Populated in later mapping stages
            table_rows,
            match_rate,
            warnings,
        })
    }

    /// Verifies if the joined columns on the target table are covered by an index.
    async fn verify_index_coverage(
        &self,
        table: &str,
        clause: &JoinClause,
        ctx: &AnalysisContext,
    ) -> bool {
        let join_columns: Vec<String> = clause
            .conditions
            .iter()
            .map(|c| c.right.column.clone())
            .collect();

        ctx.source_cache
            .are_columns_indexed(table, &join_columns)
            .await
    }

    /// Renders the JOIN ON expression into SQL using the context's source dialect.
    fn render_join_sql(&self, clause: &JoinClause, ctx: &AnalysisContext) -> String {
        match join_on_expr!(clause) {
            Ok(expr) => {
                let mut renderer = Renderer::new(ctx.source_dialect.as_ref());
                expr.render(&mut renderer);
                renderer.finish().0
            }
            Err(_) => String::new(),
        }
    }

    /// Maps internal join conditions into the final JoinPlan format.
    fn map_conditions(
        &self,
        _join: &Join,
        clause: &JoinClause,
        expression: &str,
        indexed: bool,
    ) -> Vec<JoinCondition> {
        clause
            .conditions
            .iter()
            .map(|c| JoinCondition {
                left: JoinColumn {
                    table: c.left.alias.clone(), // Source table from join condition
                    column: c.left.column.clone(),
                },
                right: JoinColumn {
                    table: c.right.alias.clone(), // Joined table from join condition
                    column: c.right.column.clone(),
                },
                expression: expression.to_string(),
                indexed,
            })
            .collect()
    }

    /// Provides simple match rate heuristics (could be expanded with statistical analysis).
    async fn estimate_match_rate(&self, _conditions: &[CoreJoinCondition]) -> Option<f32> {
        // Future implementation: Use histogram data from cache to refine this
        Some(0.95)
    }

    /// Generates human-readable warnings for unindexed joins or large scans.
    fn performance_warnings(&self, table: &str, clause: &JoinClause, indexed: bool) -> Vec<String> {
        let mut warnings = Vec::new();
        if !indexed {
            for condition in &clause.conditions {
                warnings.push(format!(
                    "Performance Risk: Join on {}.{} is not indexed. This may lead to nested loop scans on larger datasets.",
                    table, condition.right.column
                ));
            }
        }
        warnings
    }
}

#[async_trait]
impl PlanAnalyzer for JoinAnalyzer {
    type Input = Vec<Join>;
    type Output = Vec<JoinPlan>;

    fn name(&self) -> &'static str {
        "join"
    }

    /// Entry point for the PlanBuilder analyzer registry.
    async fn analyze(
        &self,
        joins: &Self::Input,
        ctx: &AnalysisContext,
    ) -> AnalyzerResult<Self::Output> {
        let mut plans = Vec::with_capacity(joins.len());

        for join in joins {
            let plan = self.analyze_single_join(join, ctx).await.map_err(|e| {
                AnalyzerError::error("join", format!("Join analysis failed: {}", e))
            })?;
            plans.push(plan);
        }

        Ok(plans)
    }
}
