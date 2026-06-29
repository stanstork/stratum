use crate::{
    error::CliError,
    tui::{pipeline::PipelineState, planner::initialize_pipelines_from_plan},
};
use engine_core::{context::env::EnvContext, plan::execution::ExecutionPlan as CoreExecutionPlan};
use engine_planner::{builder::ReportBuilder, plan::execution::migration_report::MigrationReport};
use engine_runtime::dag::{Dag, builder::DagBuilder};
use smql_syntax::builder::parse;
use std::{collections::HashMap, path::Path, sync::Arc};
use tracing::info;

/// Context containing all plan-related data needed for TUI execution
pub struct PlanContext {
    pub core_plan: CoreExecutionPlan,
    pub report: MigrationReport,
    pub dag: Dag,
    pub pipelines: HashMap<String, PipelineState>,
}

/// Builds execution plan from SMQL configuration file
pub async fn build_plan_context(
    config_path: &str,
    exact_filter: bool,
    env: Arc<EnvContext>,
) -> Result<PlanContext, CliError> {
    info!(config = %config_path, "building execution plan");

    // Parse SMQL
    let smql_content = std::fs::read_to_string(config_path)?;
    let ast = parse(&smql_content)?;

    // Build core plan
    let mut core_plan = CoreExecutionPlan::build(&ast, env)?;
    core_plan.config_path = config_path.to_string();

    // Build DAG
    let dag = build_dag(&core_plan)?;

    // Build detailed report with exact_filter configuration
    let report_config = engine_planner::builder::ReportBuilderConfig {
        exact_where: exact_filter,
        ..Default::default()
    };
    let report = ReportBuilder::new(report_config)
        .build(&core_plan, &dag, Path::new(config_path))
        .await?;

    // Initialize pipeline states
    let pipelines = initialize_pipelines_from_plan(&report, core_plan.hash());

    info!(pipelines = pipelines.len(), "plan built");

    Ok(PlanContext {
        core_plan,
        report,
        dag,
        pipelines,
    })
}

/// Builds DAG from execution plan
fn build_dag(plan: &CoreExecutionPlan) -> Result<Dag, CliError> {
    let mut dag_builder = DagBuilder::new();

    for pipeline in &plan.pipelines {
        dag_builder.add_pipeline(pipeline.name.clone(), pipeline.dependencies.clone())?;
    }

    Ok(dag_builder.build()?)
}
