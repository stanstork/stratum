use crate::{
    builder::{
        analysis::{AnalysisContext, AnalyzerError, AnalyzerResult, PlanAnalyzer},
        errors::PaginationAnalyzerError,
        utils::ColumnRefParser,
    },
    plan::pagination::{cursor::CursorColumn, plan::PaginationPlan, strategy::PaginationStrategy},
};
use async_trait::async_trait;
use engine_processing::io::driver::SchemaDriver;
use model::execution::pipeline::Pagination;
use tracing::{info, warn};

/// Analyzes pagination configuration to determine performance risks and strategy compatibility.
pub struct PaginationAnalyzer;

impl PaginationAnalyzer {
    /// Primary logic for analyzing a configured pagination strategy.
    async fn analyze_pagination_config<S: SchemaDriver, D: SchemaDriver>(
        &self,
        table: &str,
        pagination: &Pagination,
        ctx: &AnalysisContext<S, D>,
    ) -> Result<PaginationPlan, PaginationAnalyzerError> {
        let strategy = self.map_strategy_type(&pagination.strategy)?;
        let cursor_column = self.resolve_cursor_ref(&pagination.column, table)?;
        self.verify_column_metadata(&cursor_column, ctx)?;

        let tiebreaker = if let Some(tb) = &pagination.tiebreaker {
            let tb_col = self.resolve_cursor_ref(tb, table)?;
            self.verify_column_metadata(&tb_col, ctx)?;
            Some(tb_col)
        } else {
            None
        };

        let column_indexed = ctx
            .source_cache
            .is_column_indexed(&cursor_column.table, &cursor_column.column)
            .await;

        if !column_indexed && strategy != PaginationStrategy::Default {
            warn!(
                target: "analyzer",
                table = %cursor_column.table,
                column = %cursor_column.column,
                "pagination performance risk: cursor column is not indexed"
            );
        }

        info!(
            target: "analyzer",
            table = %cursor_column.table,
            strategy = ?strategy,
            cursor = %cursor_column.column,
            indexed = column_indexed,
            "Pagination analysis complete"
        );

        Ok(PaginationPlan {
            strategy,
            cursor_column: Some(cursor_column),
            tiebreaker,
            timezone: pagination.timezone.clone(),
            column_indexed: Some(column_indexed),
        })
    }

    fn map_strategy_type(
        &self,
        strategy: &str,
    ) -> Result<PaginationStrategy, PaginationAnalyzerError> {
        match strategy.to_lowercase().as_str() {
            "timestamp" => Ok(PaginationStrategy::Timestamp),
            "numeric" => Ok(PaginationStrategy::Numeric),
            "pk" => Ok(PaginationStrategy::Pk),
            "default" | "offset" => Ok(PaginationStrategy::Default),
            _ => Err(PaginationAnalyzerError::UnsupportedStrategy {
                strategy: strategy.to_string(),
            }),
        }
    }

    fn resolve_cursor_ref(
        &self,
        column: &str,
        default_table: &str,
    ) -> Result<CursorColumn, PaginationAnalyzerError> {
        if column.is_empty() {
            return Err(PaginationAnalyzerError::InvalidCursor {
                cursor: "EMPTY".into(),
                reason: "Cursor column name cannot be empty".into(),
            });
        }

        let parsed = ColumnRefParser::parse(column, default_table).map_err(|e| {
            PaginationAnalyzerError::InvalidCursor {
                cursor: column.to_string(),
                reason: format!("Failed to parse column reference: {}", e),
            }
        })?;

        Ok(CursorColumn {
            table: parsed.table,
            column: parsed.column,
        })
    }

    fn verify_column_metadata<S: SchemaDriver, D: SchemaDriver>(
        &self,
        cursor: &CursorColumn,
        ctx: &AnalysisContext<S, D>,
    ) -> Result<(), PaginationAnalyzerError> {
        let table_meta = ctx
            .schema_plan
            .metadata_graph()
            .get(&cursor.table)
            .ok_or_else(|| PaginationAnalyzerError::MetadataError {
                table: cursor.table.clone(),
                reason: "Table metadata not found in plan graph.".into(),
            })?;

        let column_exists = table_meta
            .columns()
            .iter()
            .any(|col| col.name == cursor.column);

        if !column_exists {
            return Err(PaginationAnalyzerError::CursorColumnNotFound {
                table: cursor.table.clone(),
                column: cursor.column.clone(),
            });
        }

        Ok(())
    }
}

#[async_trait]
impl<S: SchemaDriver, D: SchemaDriver> PlanAnalyzer<S, D> for PaginationAnalyzer {
    type Input = (String, Option<Pagination>);
    type Output = Option<PaginationPlan>;

    fn name(&self) -> &'static str {
        "pagination"
    }

    async fn analyze(
        &self,
        input: &Self::Input,
        ctx: &AnalysisContext<S, D>,
    ) -> AnalyzerResult<Self::Output> {
        let (table_name, config) = input;

        match config {
            Some(pagination) => {
                let plan = self
                    .analyze_pagination_config(table_name, pagination, ctx)
                    .await
                    .map_err(|e| AnalyzerError::error("pagination", e.to_string()))?;
                Ok(Some(plan))
            }
            None => Ok(None),
        }
    }
}
