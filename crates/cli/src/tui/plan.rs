use crate::{
    error::CliError,
    tui::{pipeline::PipelineState, planner::initialize_pipelines_from_plan},
};
use engine_core::plan::execution::ExecutionPlan as CoreExecutionPlan;
use engine_planner::{builder::PlanBuilder, plan::execution::execution_plan::ExecutionPlan};
use engine_runtime::dag::{Dag, builder::DagBuilder};
use smql_syntax::builder::parse;
use std::{collections::HashMap, path::Path};
use tracing::info;

/// Context containing all plan-related data needed for TUI execution
pub struct PlanContext {
    pub core_plan: CoreExecutionPlan,
    pub planner_plan: ExecutionPlan,
    pub dag: Dag,
    pub pipelines: HashMap<String, PipelineState>,
}

/// Builds execution plan from SMQL configuration file
pub async fn build_plan_context(
    config_path: &str,
    exact_filter: bool,
) -> Result<PlanContext, CliError> {
    info!("Building execution plan from {}...", config_path);

    // Parse SMQL
    let smql_content = std::fs::read_to_string(config_path)?;
    let ast = parse(&smql_content)?;

    // Build core plan
    let core_plan = CoreExecutionPlan::build(&ast)?;

    // Build DAG
    let dag = build_dag(&core_plan)?;

    // Build detailed plan with exact_filter configuration
    let plan_config = engine_planner::builder::PlanBuilderConfig {
        exact_where: exact_filter,
        ..Default::default()
    };
    let planner_plan = PlanBuilder::new(plan_config)
        .build(&core_plan, &dag, Path::new(config_path))
        .await?;

    // Initialize pipeline states
    let pipelines = initialize_pipelines_from_plan(&planner_plan, &core_plan.hash());

    info!("Plan built successfully with {} pipelines", pipelines.len());

    Ok(PlanContext {
        core_plan,
        planner_plan,
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
