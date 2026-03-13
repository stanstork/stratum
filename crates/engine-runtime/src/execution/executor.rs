use crate::{
    dag::{builder::DagBuilder, executor::DagExecutor},
    error::MigrationError,
};
use engine_core::{context::env::EnvContext, plan::execution::ExecutionPlan};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::info;

pub async fn run(
    plan: ExecutionPlan,
    dry_run: bool,
    cancel: CancellationToken,
    env: Arc<EnvContext>,
) -> Result<(), MigrationError> {
    // Build DAG from the execution plan
    let mut builder = DagBuilder::new();

    for pipeline in &plan.pipelines {
        builder.add_pipeline(pipeline.name.clone(), pipeline.dependencies.clone())?;
    }

    let dag = builder.build()?;

    info!("DAG built successfully:");
    info!("  Total pipelines: {}", dag.total_pipelines());
    info!("  Execution levels: {}", dag.execution_order().len());
    info!("  Max parallelism: {}", dag.max_parallelism());

    // Print execution plan
    for (level_idx, level) in dag.execution_order().iter().enumerate() {
        info!("  Level {}: {:?}", level_idx + 1, level);
    }

    DagExecutor::new(plan, dry_run, cancel, env)
        .await?
        .execute(dag)
        .await
}
