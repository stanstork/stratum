use crate::{
    dag::{builder::DagBuilder, executor::DagExecutor},
    error::MigrationError,
};
use engine_core::{context::env::EnvContext, plan::execution::ExecutionPlan};
use engine_infra::shutdown::ShutdownSignal;
use model::execution::flags::ExecutionFlags;
use std::sync::Arc;
use tracing::{debug, info};

pub async fn run(
    plan: ExecutionPlan,
    flags: ExecutionFlags,
    shutdown: ShutdownSignal,
    env: Arc<EnvContext>,
) -> Result<(), MigrationError> {
    // Build DAG from the execution plan
    let mut builder = DagBuilder::new();

    for pipeline in &plan.pipelines {
        builder.add_pipeline(pipeline.name.clone(), pipeline.dependencies.clone())?;
    }

    let dag = builder.build()?;

    info!(
        pipelines = dag.total_pipelines(),
        levels = dag.execution_order().len(),
        max_parallelism = dag.max_parallelism(),
        "DAG built"
    );

    for (level_idx, level) in dag.execution_order().iter().enumerate() {
        debug!(level = level_idx + 1, pipelines = ?level, "execution level");
    }

    DagExecutor::new(plan, flags, shutdown, env)
        .await?
        .execute(dag)
        .await
}
