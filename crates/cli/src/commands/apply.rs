use crate::{config, error::CliError, pretty_printer::PrettyPrinter, tui::orchestrator::run_tui};
use engine_core::{
    context::env::EnvContext, event_bus::bus::EventBus, plan::execution::ExecutionPlan,
    utils::make_item_id,
};
use engine_infra::shutdown::ShutdownSignal;
use engine_runtime::{
    dag::{Dag, builder::DagBuilder, executor::DagExecutor},
    error::MigrationError,
    execution::executor,
};
use model::execution::flags::{ExecutionFlags, IntegrityMode};
use std::{path::PathBuf, sync::Arc};
use tracing::{error, info, warn};

/// Executes the apply command (run migration)
pub async fn execute(
    config_path: Option<String>,
    tui: bool,
    pretty: bool,
    exact_filter: bool,
    integrity: IntegrityMode,
    shutdown: ShutdownSignal,
    env: Arc<EnvContext>,
) -> Result<(), CliError> {
    // Validate mutually exclusive modes
    if tui && pretty {
        return Err(CliError::Unknown(
            "Cannot use both --tui and --pretty modes simultaneously".to_string(),
        ));
    }

    let config_path = config::resolve_path(config_path)?;
    let flags = ExecutionFlags::new(false, integrity);

    // Watch for pause sentinel file. Dropping the watcher cleans up the file.
    let _pause_watcher = PauseWatcher::start(&config_path, &shutdown, env.clone()).await?;

    match (tui, pretty) {
        (true, _) => run_tui_mode(config_path, flags, exact_filter, shutdown, env).await,
        (_, true) => run_pretty_mode(config_path, flags, exact_filter, shutdown, env).await,
        _ => run_headless_mode(config_path, flags, shutdown, env).await,
    }
}

/// Runs migration in TUI mode
async fn run_tui_mode(
    config_path: String,
    flags: ExecutionFlags,
    exact_filter: bool,
    shutdown: ShutdownSignal,
    env: Arc<EnvContext>,
) -> Result<(), CliError> {
    info!(config = %config_path, "running migration with TUI");
    run_tui(config_path, exact_filter, flags.integrity, shutdown, env).await
}

/// Runs migration with pretty output
async fn run_pretty_mode(
    config_path: String,
    flags: ExecutionFlags,
    exact_filter: bool,
    shutdown: ShutdownSignal,
    env: Arc<EnvContext>,
) -> Result<(), CliError> {
    let plan = config::load_plan(&config_path, exact_filter, env.clone()).await?;
    let event_bus = EventBus::new();
    let pipeline_names = build_pipeline_name_mapping(&plan);

    // Spawn pretty printer task
    let printer_cancel = shutdown.cancel.clone();
    let printer_bus = event_bus.clone();
    let printer_handle = tokio::spawn(async move {
        if let Err(e) = PrettyPrinter::run(printer_bus, printer_cancel, pipeline_names).await {
            error!(error = %e, "pretty printer error");
        }
    });

    let dag = build_dag(&plan)?;
    let executor = DagExecutor::with_event_bus(plan, flags, shutdown, event_bus, env)
        .await
        .map_err(CliError::Migration)?;

    let result = executor.execute(dag).await;

    // Wait for printer to finish
    let _ = printer_handle.await;

    handle_execution_result(result)
}

/// Runs migration in headless mode (no TUI, no pretty output)
async fn run_headless_mode(
    config_path: String,
    flags: ExecutionFlags,
    shutdown: ShutdownSignal,
    env: Arc<EnvContext>,
) -> Result<(), CliError> {
    info!(config = %config_path, "executing migration");
    let plan = config::load_plan(&config_path, false, env.clone()).await?;
    handle_execution_result(executor::run(plan, flags, shutdown, env).await)
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

/// Handles execution result consistently across modes
fn handle_execution_result(result: Result<(), MigrationError>) -> Result<(), CliError> {
    match result {
        Ok(_) => {
            info!("migration completed successfully");
            Ok(())
        }
        Err(MigrationError::Paused) => {
            info!("migration paused, resume with the same config to continue");
            Err(CliError::Paused)
        }
        Err(MigrationError::ShutdownRequested) => {
            info!("migration stopped by shutdown request, progress saved");
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
        let item_id = make_item_id(plan_hash, &pipeline.destination.table, idx);
        mapping.insert(item_id, pipeline.name.clone());
    }

    mapping
}

/// Watches for a pause sentinel file and cancels the pause token when found.
/// Also cleans up the sentinel file on drop.
struct PauseWatcher {
    pause_path: Option<PathBuf>,
    _handle: Option<tokio::task::JoinHandle<()>>,
}

impl PauseWatcher {
    /// Starts a background task that polls for `{run_id}.pause` sentinel file.
    async fn start(
        config_path: &str,
        shutdown: &ShutdownSignal,
        env: Arc<EnvContext>,
    ) -> Result<Self, CliError> {
        let plan = config::load_plan(config_path, false, env).await?;
        let run_id = plan.run_id();

        let dir = match super::state_dir() {
            Ok(d) => d,
            Err(_) => {
                return Ok(Self {
                    pause_path: None,
                    _handle: None,
                });
            }
        };

        if let Err(e) = std::fs::create_dir_all(&dir) {
            warn!(dir = %dir.display(), error = %e, "failed to create state directory");
            return Ok(Self {
                pause_path: None,
                _handle: None,
            });
        }

        let pause_path = dir.join(format!("{run_id}.pause"));

        // Clean up any stale pause file from a previous run
        let _ = std::fs::remove_file(&pause_path);

        let watch_path = pause_path.clone();
        let pause_token = shutdown.pause.clone();
        let cancel_token = shutdown.cancel.clone();

        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(500));
            loop {
                interval.tick().await;

                // Stop watching if migration is already done or pausing
                if cancel_token.is_cancelled() || pause_token.is_cancelled() {
                    break;
                }

                if watch_path.exists() {
                    info!("pause sentinel file detected, requesting pause");
                    pause_token.cancel();
                    break;
                }
            }
        });

        Ok(Self {
            pause_path: Some(pause_path),
            _handle: Some(handle),
        })
    }
}

impl Drop for PauseWatcher {
    fn drop(&mut self) {
        if let Some(path) = &self.pause_path {
            let _ = std::fs::remove_file(path);
        }
    }
}
