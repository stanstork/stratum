use crate::plan::execution::types::RowCount;
use serde::Serialize;

/// High-level plan summary
#[derive(Serialize, Debug, Clone, Default)]
pub struct PlanSummary {
    pub total_pipelines: usize,
    pub total_connections: usize,
    pub total_source_rows: RowCount,
    pub total_target_rows: RowCount,
    pub total_schema_changes: usize,
    pub status: PlanStatus,
    pub error_count: usize,
    pub warning_count: usize,
}

#[derive(Serialize, Debug, Clone, PartialEq, Default)]
#[serde(rename_all = "snake_case")]
pub enum PlanStatus {
    #[default]
    Ready,
    ReadyWithWarnings,
    NotExecutable,
}
