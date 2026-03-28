use crate::{config, error::CliError, pretty_printer::PrettyPrinter, tui::orchestrator::run_tui};
use engine_core::{
    context::env::EnvContext, event_bus::bus::EventBus, plan::execution::ExecutionPlan,
    utils::make_item_id,
};
use engine_runtime::{
    dag::{Dag, builder::DagBuilder, executor::DagExecutor},
    error::MigrationError,
    execution::executor,
};
use model::execution::flags::{ExecutionFlags, IntegrityMode};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::info;

/// Executes the apply command (run migration)
pub async fn execute(
    config_path: Option<String>,
    tui: bool,
    pretty: bool,
    exact_filter: bool,
    integrity: IntegrityMode,
    cancel: CancellationToken,
    env: Arc<EnvContext>,
) -> Result<(), CliError> {
    let config_path = config::resolve_path(config_path)?;

    // Validate mutually exclusive modes
    if tui && pretty {
        return Err(CliError::Unknown(
            "Cannot use both --tui and --pretty modes simultaneously".to_string(),
        ));
    }

    let flags = ExecutionFlags::new(false, integrity);

    match (tui, pretty) {
        (true, _) => run_tui_mode(config_path, flags, exact_filter, cancel, env).await,
        (_, true) => run_pretty_mode(config_path, flags, exact_filter, cancel, env).await,
        _ => run_headless_mode(config_path, flags, cancel, env).await,
    }
}

/// Runs migration in TUI mode
async fn run_tui_mode(
    config_path: String,
    flags: ExecutionFlags,
    exact_filter: bool,
    cancel: CancellationToken,
    env: Arc<EnvContext>,
) -> Result<(), CliError> {
    info!("Running migration with TUI: {}", config_path);
    run_tui(config_path, exact_filter, flags.integrity, cancel, env).await
}

/// Runs migration with pretty output
async fn run_pretty_mode(
    config_path: String,
    flags: ExecutionFlags,
    exact_filter: bool,
    cancel: CancellationToken,
    env: Arc<EnvContext>,
) -> Result<(), CliError> {
    let plan = config::load_plan(&config_path, exact_filter, env.clone()).await?;
    let event_bus = EventBus::new();

    // Build item_id -> pipeline name mapping
    let pipeline_names = build_pipeline_name_mapping(&plan);

    // Spawn pretty printer task
    let printer_cancel = cancel.clone();
    let printer_bus = event_bus.clone();
    let printer_handle = tokio::spawn(async move {
        if let Err(e) = PrettyPrinter::run(printer_bus, printer_cancel, pipeline_names).await {
            tracing::error!("Pretty printer error: {}", e);
        }
    });

    // Build DAG and execute
    let dag = build_dag(&plan)?;
    let executor = create_executor(flags, plan, cancel.clone(), event_bus, env).await?;
    let result = executor.execute(dag).await;

    // Wait for printer to finish
    let _ = printer_handle.await;

    handle_execution_result(result)
}

/// Runs migration in headless mode (no TUI, no pretty output)
async fn run_headless_mode(
    config_path: String,
    flags: ExecutionFlags,
    cancel: CancellationToken,
    env: Arc<EnvContext>,
) -> Result<(), CliError> {
    info!("Executing migration: {}", config_path);

    let plan = config::load_plan(&config_path, false, env.clone()).await?;
    let result = executor::run(plan, flags, cancel, env).await;

    handle_execution_result(result)
}

/// Builds execution DAG from plan
fn build_dag(plan: &ExecutionPlan) -> Result<Dag, CliError> {
    let mut builder = DagBuilder::new();

    for pipeline in &plan.pipelines {
        builder
            .add_pipeline(pipeline.name.clone(), pipeline.dependencies.clone())
            .map_err(|e| CliError::Migration(MigrationError::Dag(e)))?;
    }

    builder
        .build()
        .map_err(|e| CliError::Migration(MigrationError::Dag(e)))
}

/// Creates executor with event bus
async fn create_executor(
    flags: ExecutionFlags,
    plan: ExecutionPlan,
    cancel: CancellationToken,
    event_bus: EventBus,
    env: Arc<EnvContext>,
) -> Result<DagExecutor, CliError> {
    DagExecutor::with_event_bus(plan, flags, cancel, event_bus, env)
        .await
        .map_err(CliError::Migration)
}

/// Handles execution result consistently across modes
fn handle_execution_result(result: Result<(), MigrationError>) -> Result<(), CliError> {
    match result {
        Ok(_) => {
            info!("Migration completed successfully");
            Ok(())
        }
        Err(MigrationError::ShutdownRequested) => {
            info!("Migration stopped due to shutdown request - progress has been saved");
            Err(CliError::ShutdownRequested)
        }
        Err(e) => Err(CliError::Migration(e)),
    }
}

/// Builds a mapping from item_id to pipeline name for display purposes
fn build_pipeline_name_mapping(
    plan: &engine_core::plan::execution::ExecutionPlan,
) -> std::collections::HashMap<String, String> {
    let mut mapping = std::collections::HashMap::new();
    let plan_hash = plan.hash();

    for (idx, pipeline) in plan.pipelines.iter().enumerate() {
        let item_id = make_item_id(&plan_hash, &pipeline.destination.table, idx);
        mapping.insert(item_id, pipeline.name.clone());
    }

    mapping
}
