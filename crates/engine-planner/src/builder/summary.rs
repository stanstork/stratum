use model::execution::row_count::RowCount;

use crate::plan::{
    connection::plan::ConnectionPlan,
    diagnostics::level::DiagnosticLevel,
    execution::summary::{PlanStatus, PlanSummary},
    pipeline::plan::PipelinePlan,
};

/// Orchestrates the high-level summary of the execution plan
pub struct SummaryCalculator;

impl SummaryCalculator {
    pub fn calculate(pipelines: &[PipelinePlan], connections: &[ConnectionPlan]) -> PlanSummary {
        let total_source_rows =
            RowCount::sum(pipelines.iter().map(|p| p.source.effective_row_count()));
        let total_target_rows =
            RowCount::sum(pipelines.iter().map(|p| &p.destination.current_rows));
        let total_schema_changes: usize = pipelines.iter().map(|p| p.schema_changes.len()).sum();

        let diagnostics = pipelines.iter().flat_map(|p| &p.diagnostics);
        let error_count = diagnostics
            .clone()
            .filter(|d| d.level == DiagnosticLevel::Error)
            .count();
        let warning_count = diagnostics
            .filter(|d| d.level == DiagnosticLevel::Warning)
            .count();

        let status = if error_count > 0 {
            PlanStatus::NotExecutable
        } else if warning_count > 0 {
            PlanStatus::ReadyWithWarnings
        } else {
            PlanStatus::Ready
        };

        PlanSummary {
            total_pipelines: pipelines.len(),
            total_connections: connections.len(),
            total_source_rows,
            total_target_rows,
            total_schema_changes,
            status,
            error_count,
            warning_count,
        }
    }
}
