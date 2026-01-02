use crate::plan::{
    diagnostics::diagnostic::Diagnostic,
    error_handling::plan::ErrorHandlingPlan,
    estimation::pipeline::PipelineEstimations,
    hooks::plan::HooksPlan,
    pagination::plan::PaginationPlan,
    pipeline::{
        data_flow_summary::DataFlowSummary, destination::DestinationPlan,
        settings::PipelineSettings, source::SourcePlan,
    },
    sample::preview::SampleDataPreview,
    schema::change::SchemaChange,
    transform::{filter::FilterPlan, join::JoinPlan, mapping::ColumnMapping},
    validation::plan::ValidationPlan,
};
use serde::Serialize;

#[derive(Serialize, Debug, Clone)]
pub struct PipelinePlan {
    // ─── Identity ────
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    // ─── Execution Order ────
    /// Order in which this pipeline executes (0-indexed)
    pub execution_order: usize,

    /// Which parallel stage this belongs to
    pub execution_stage: usize,

    pub depends_on: Vec<String>,

    // ─── Data Flow ────
    pub source: SourcePlan,
    pub destination: DestinationPlan,
    pub filters: Vec<FilterPlan>,
    pub joins: Vec<JoinPlan>,
    pub mappings: Vec<ColumnMapping>,
    pub validations: Vec<ValidationPlan>,

    // ─── Error Handling ────
    pub error_handling: ErrorHandlingPlan,

    // ─── Pagination ────
    pub pagination: PaginationPlan,

    // ─── Hooks ────
    pub hooks: HooksPlan,

    // ─── Settings ────
    pub settings: PipelineSettings,

    // ─── Computed ────
    pub data_flow_summary: DataFlowSummary,
    pub schema_changes: Vec<SchemaChange>,
    pub diagnostics: Vec<Diagnostic>,
    pub estimations: PipelineEstimations,

    // ─── Sample Data (optional) ────
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sample: Option<SampleDataPreview>,
}
