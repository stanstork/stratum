use crate::{
    builder::analysis::{AnalysisContext, AnalyzerResult, PlanAnalyzer},
    plan::{
        connection::plan::DatabaseDriver,
        execution::types::RowCount,
        pipeline::{
            destination::{DataImpact, DataImpactAction, DestinationPlan, WriteMode},
            source::ColumnInfo,
        },
    },
};
use async_trait::async_trait;
use model::execution::pipeline::DataDestination;
use model::execution::pipeline::WriteMode as CoreWriteMode;
use tracing::{info, warn};

/// Analyzes destination tables to gather metadata and determine write impact
pub struct DestinationAnalyzer;

impl DestinationAnalyzer {
    /// Determine conflict keys for upsert/merge operations
    async fn determine_conflict_keys(
        &self,
        table: &str,
        mode: &CoreWriteMode,
        ctx: &AnalysisContext,
    ) -> Vec<String> {
        match mode {
            CoreWriteMode::Upsert | CoreWriteMode::Update => {
                // Attempt to fetch primary keys from the metadata cache if it's a key-dependent operation
                match ctx.dest_cache.table_metadata(table).await {
                    Ok(meta) => meta.primary_keys.to_vec(),
                    Err(_) => Vec::new(),
                }
            }
            _ => Vec::new(),
        }
    }

    /// Calculate the impact of the write operation on existing data
    fn calculate_data_impact(
        &self,
        mode: &WriteMode,
        current_rows: &RowCount,
        table_exists: bool,
    ) -> DataImpact {
        if !table_exists {
            return DataImpact {
                action: DataImpactAction::Create,
                description: "Table will be created with new data".to_string(),
                is_destructive: false,
                affected_rows: None,
            };
        }

        match mode {
            WriteMode::Replace => DataImpact {
                action: DataImpactAction::Truncate,
                description: "All existing data will be deleted, then new data inserted"
                    .to_string(),
                is_destructive: true,
                affected_rows: Some(current_rows.clone()),
            },
            WriteMode::Append => DataImpact {
                action: DataImpactAction::Append,
                description: "New rows will be appended, existing data preserved".to_string(),
                is_destructive: false,
                affected_rows: None,
            },
            WriteMode::Upsert => DataImpact {
                action: DataImpactAction::Upsert,
                description:
                    "Rows will be inserted or updated based on conflict keys, no data loss"
                        .to_string(),
                is_destructive: false,
                affected_rows: None,
            },
            WriteMode::Merge => DataImpact {
                action: DataImpactAction::Merge,
                description: "Conditional merge operation, may update existing rows".to_string(),
                is_destructive: false,
                affected_rows: None,
            },
        }
    }

    async fn fetch_destination_columns(
        &self,
        table: &str,
        driver: &DatabaseDriver,
        ctx: &AnalysisContext,
    ) -> Vec<ColumnInfo> {
        match ctx.dest_cache.table_metadata(table).await {
            Ok(metadata) => metadata
                .columns()
                .iter()
                .map(|col| ColumnInfo::from_metadata(col, driver))
                .collect(),
            Err(e) => {
                warn!(table = %table, error = %e, "Failed to fetch destination columns");
                Vec::new()
            }
        }
    }
}

#[async_trait]
impl PlanAnalyzer for DestinationAnalyzer {
    type Input = DataDestination;
    type Output = DestinationPlan;

    fn name(&self) -> &'static str {
        "destination"
    }

    async fn analyze(
        &self,
        destination: &Self::Input,
        ctx: &AnalysisContext,
    ) -> AnalyzerResult<Self::Output> {
        info!(table = %destination.table, "Analyzing destination table");

        let driver = DatabaseDriver::from_name(&destination.connection.driver);
        let table_exists = ctx
            .dest_cache
            .table_exists(&destination.table)
            .await
            .unwrap_or(false);

        let (current_rows, columns) = if table_exists {
            let rows = ctx.dest_cache.count_rows(&destination.table, None).await;
            let cols = self
                .fetch_destination_columns(&destination.table, &driver, ctx)
                .await;
            (rows, cols)
        } else {
            (RowCount::unknown(), Vec::new())
        };

        let write_mode = WriteMode::from(&destination.mode);
        let conflict_keys = self
            .determine_conflict_keys(&destination.table, &destination.mode, ctx)
            .await;
        let data_impact = self.calculate_data_impact(&write_mode, &current_rows, table_exists);

        let plan = DestinationPlan {
            connection: destination.connection.name.clone(),
            table: destination.table.clone(),
            schema: None,
            fqn: destination.table.clone(),
            driver,
            exists: table_exists,
            current_rows,
            mode: write_mode,
            conflict_keys,
            columns,
            data_impact,
        };

        info!(
            target: "analyzer",
            table = %plan.table,
            exists = %plan.exists,
            mode = ?plan.mode,
            impact = ?plan.data_impact.action,
            "Destination analysis completed"
        );

        Ok(plan)
    }
}

impl From<&CoreWriteMode> for WriteMode {
    fn from(mode: &CoreWriteMode) -> Self {
        match mode {
            CoreWriteMode::Replace => WriteMode::Replace,
            CoreWriteMode::Insert => WriteMode::Append,
            CoreWriteMode::Upsert => WriteMode::Upsert,
            CoreWriteMode::Update => WriteMode::Merge,
        }
    }
}
