use crate::{
    builder::analysis::{AnalysisContext, AnalyzerError, AnalyzerResult, PlanAnalyzer},
    plan::schema::{change::SchemaChange, types::SchemaChangeType},
};
use async_trait::async_trait;
use connectors::sql::base::query::generator::QueryGenerator;
use model::execution::pipeline::Pipeline;
use tracing::info;

/// Analyzes differences between the source schema and destination table structure
/// to generate a list of required schema migrations.
pub struct SchemaAnalyzer;

impl SchemaAnalyzer {
    /// Primary logic for determining schema changes based on table existence.
    async fn analyze_pipeline_schema(
        &self,
        pipeline: &Pipeline,
        ctx: &AnalysisContext,
    ) -> AnalyzerResult<Vec<SchemaChange>> {
        let dest_table = &pipeline.destination.table;

        // Check if destination table exists using the context-aware metadata cache
        let dest_exists = ctx.dest_cache.table_exists(dest_table).await.map_err(|e| {
            AnalyzerError::error("schema", format!("Failed to check table existence: {}", e))
        })?;

        if dest_exists {
            info!(target: "analyzer", table = %dest_table, "Analyzing modifications for existing table structure");
            self.compare_and_modify(dest_table, ctx).await
        } else {
            info!(target: "analyzer", table = %dest_table, "Analyzing requirements for new table creation");
            self.plan_table_creation(dest_table, ctx).await
        }
    }

    /// Generates the full set of changes required to create a new table, including enums and constraints.
    async fn plan_table_creation(
        &self,
        dest_table: &str,
        ctx: &AnalysisContext,
    ) -> AnalyzerResult<Vec<SchemaChange>> {
        let mut changes = Vec::new();

        // Core Table Creation
        let table_queries = ctx.schema_plan.table_queries().await;
        let (create_sql, _) = table_queries
            .iter()
            .find(|q| q.1.eq_ignore_ascii_case(dest_table))
            .ok_or_else(|| {
                AnalyzerError::error(
                    "schema",
                    format!("No table query generated for target: {}", dest_table),
                )
            })?;

        changes.push(SchemaChange {
            change_type: SchemaChangeType::CreateTable,
            entity: dest_table.to_string(),
            description: format!("Create new table '{}'", dest_table),
            ddl: Some(create_sql.clone()),
            is_breaking: false,
            is_reversible: true,
        });

        // Custom Enum Types
        let enum_queries = ctx.schema_plan.enum_queries().await.map_err(|e| {
            AnalyzerError::error("schema", format!("Enum query generation failed: {}", e))
        })?;

        for (sql, column_name) in enum_queries {
            changes.push(SchemaChange {
                change_type: SchemaChangeType::CreateEnum,
                entity: format!("{}.{}", dest_table, column_name),
                description: format!("Create custom enum type for column '{}'", column_name),
                ddl: Some(sql),
                is_breaking: false,
                is_reversible: true,
            });
        }

        // Foreign Key Constraints
        for (sql, column_name) in ctx.schema_plan.fk_queries() {
            changes.push(SchemaChange {
                change_type: SchemaChangeType::AddConstraint,
                entity: format!("{}.{}", dest_table, column_name),
                description: format!("Add foreign key constraint to column '{}'", column_name),
                ddl: Some(sql),
                is_breaking: false,
                is_reversible: true,
            });
        }

        info!(target: "analyzer", table = %dest_table, "Planned creation of new table with {} changes", changes.len());

        Ok(changes)
    }

    /// Compares the planned column definitions with existing physical metadata to find missing columns.
    async fn compare_and_modify(
        &self,
        dest_table: &str,
        ctx: &AnalysisContext,
    ) -> AnalyzerResult<Vec<SchemaChange>> {
        let mut changes = Vec::new();

        // Fetch current physical metadata for comparison
        let dest_metadata = ctx
            .dest_cache
            .table_metadata(dest_table)
            .await
            .map_err(|e| {
                AnalyzerError::error(
                    "schema",
                    format!("Metadata retrieval failed for {}: {}", dest_table, e),
                )
            })?;

        let planned_columns = ctx.schema_plan.resolved_column_defs().await;
        let existing_columns = dest_metadata.columns();

        // Use the destination dialect from context to generate appropriate ALTER statements
        let generator = QueryGenerator::new(ctx.dest_dialect.as_ref());

        for planned_col in planned_columns {
            let col_name = planned_col.name();
            let exists = existing_columns
                .iter()
                .any(|c| c.name.eq_ignore_ascii_case(col_name));

            if !exists {
                let (sql, _) = generator.add_column(dest_table, planned_col.clone());

                changes.push(SchemaChange {
                    change_type: SchemaChangeType::AddColumn,
                    entity: format!("{}.{}", dest_table, col_name),
                    description: format!(
                        "Add missing column '{}' to table '{}'",
                        col_name, dest_table
                    ),
                    ddl: Some(sql),
                    is_breaking: false,
                    is_reversible: true,
                });
            }
        }

        if changes.is_empty() {
            info!(target: "analyzer", table = %dest_table, "Schema matches target plan; no changes required");
        }

        info!(target: "analyzer", table = %dest_table, "Identified {} schema changes for modification", changes.len());

        Ok(changes)
    }
}

#[async_trait]
impl PlanAnalyzer for SchemaAnalyzer {
    type Input = Pipeline;
    type Output = Vec<SchemaChange>;

    fn name(&self) -> &'static str {
        "schema"
    }

    async fn analyze(
        &self,
        pipeline: &Self::Input,
        ctx: &AnalysisContext,
    ) -> AnalyzerResult<Self::Output> {
        self.analyze_pipeline_schema(pipeline, ctx).await
    }
}
