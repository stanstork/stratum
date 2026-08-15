use crate::{
    builder::{
        analysis::{AnalysisContext, AnalyzerError, AnalyzerResult, PlanAnalyzer},
        graph,
    },
    plan::schema::{change::SchemaChange, types::SchemaChangeType},
};
use async_trait::async_trait;
use connectors::sql::query::generator::QueryGenerator;
use engine_processing::io::driver::SchemaDriver;
use model::execution::pipeline::Pipeline;
use tracing::info;

/// Analyzes differences between the source schema and destination table structure
/// to generate a list of required schema migrations.
pub struct SchemaAnalyzer;

impl SchemaAnalyzer {
    /// Primary logic for determining schema changes based on table existence.
    async fn analyze_pipeline_schema<S: SchemaDriver, D: SchemaDriver>(
        &self,
        pipeline: &Pipeline,
        ctx: &AnalysisContext<S, D>,
    ) -> AnalyzerResult<Vec<SchemaChange>> {
        let dest_table = &pipeline.destination.table;

        let dest_exists = ctx.dest_cache.table_exists(dest_table).await.map_err(|e| {
            AnalyzerError::error("schema", format!("Failed to check table existence: {}", e))
        })?;

        if dest_exists {
            info!(target: "analyzer", table = %dest_table, "analyzing modifications for existing table");
            self.compare_and_modify(dest_table, ctx).await
        } else {
            info!(target: "analyzer", table = %dest_table, "analyzing requirements for new table");
            self.plan_table_creation(dest_table, ctx).await
        }
    }

    /// Generates the full set of changes required to create a new table, including enums and constraints.
    async fn plan_table_creation<S: SchemaDriver, D: SchemaDriver>(
        &self,
        dest_table: &str,
        ctx: &AnalysisContext<S, D>,
    ) -> AnalyzerResult<Vec<SchemaChange>> {
        let changes = graph::schema_ops_to_changes(&ctx.schema_plan.build_ops());

        info!(target: "analyzer", table = %dest_table, changes = changes.len(), "planned new table creation");

        Ok(changes)
    }

    /// Compares the planned column definitions with existing physical metadata to find missing columns.
    async fn compare_and_modify<S: SchemaDriver, D: SchemaDriver>(
        &self,
        dest_table: &str,
        ctx: &AnalysisContext<S, D>,
    ) -> AnalyzerResult<Vec<SchemaChange>> {
        let dest_metadata = ctx
            .dest_cache
            .table_metadata(dest_table)
            .await
            .map_err(|e| {
                AnalyzerError::error(
                    "schema",
                    format!("Metadata retrieval failed for {dest_table}: {e}"),
                )
            })?;

        let planned_columns = ctx.schema_plan.resolved_column_defs().await;
        let existing_columns = dest_metadata.columns();

        let dialect = ctx.dest_dialect.as_query_dialect();
        let generator = QueryGenerator::new(dialect);

        let changes: Vec<SchemaChange> = planned_columns
            .into_iter()
            .filter(|planned_col| {
                !existing_columns
                    .iter()
                    .any(|c| c.name.eq_ignore_ascii_case(planned_col.name()))
            })
            .map(|planned_col| {
                let col_name = planned_col.name().to_string();
                let (sql, _) = generator.add_column(dest_table, planned_col);

                SchemaChange {
                    change_type: SchemaChangeType::AddColumn,
                    entity: format!("{dest_table}.{col_name}"),
                    description: format!("Add missing column '{col_name}' to table '{dest_table}'"),
                    ddl: Some(sql),
                    is_breaking: false,
                    is_reversible: true,
                }
            })
            .collect();

        if changes.is_empty() {
            info!(target: "analyzer", table = %dest_table, "schema matches target, no changes required");
        } else {
            info!(target: "analyzer", table = %dest_table, changes = changes.len(), "identified schema changes for modification");
        }

        Ok(changes)
    }
}

#[async_trait]
impl<S: SchemaDriver, D: SchemaDriver> PlanAnalyzer<S, D> for SchemaAnalyzer {
    type Input = Pipeline;
    type Output = Vec<SchemaChange>;

    fn name(&self) -> &'static str {
        "schema"
    }

    async fn analyze(
        &self,
        pipeline: &Self::Input,
        ctx: &AnalysisContext<S, D>,
    ) -> AnalyzerResult<Self::Output> {
        self.analyze_pipeline_schema(pipeline, ctx).await
    }
}
