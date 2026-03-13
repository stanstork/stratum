use crate::{
    error::CliError,
    tui::{
        app::{
            command::MigrationCommand,
            core::App,
            handlers::events::{TerminalEvent, spawn_terminal_events},
        },
        plan::build_plan_context,
        tasks::{spawn_command_handler, spawn_event_forwarder, spawn_executor},
        terminal::TerminalGuard,
    },
};
use engine_core::{
    context::env::EnvContext, event_bus::bus::EventBus, plan::execution::ExecutionPlan,
};
use engine_runtime::dag::Dag;
use indicatif::{ProgressBar, ProgressStyle};
use model::events::migration::MigrationEvent;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// Orchestrates the TUI lifecycle and background engine tasks
///
/// This is the main entry point for the TUI mode. It:
/// 1. Builds the execution plan from the SMQL configuration
/// 2. Initializes the terminal in alternate screen mode
/// 3. Sets up communication channels between components
/// 4. Spawns background tasks for event forwarding, command handling, and execution
/// 5. Runs the main TUI application loop
/// 6. Ensures proper terminal cleanup on exit
pub async fn run_tui(
    config_path: String,
    exact_filter: bool,
    cancel: CancellationToken,
    env: Arc<EnvContext>,
) -> Result<(), CliError> {
    // Build Plan (Outside TUI mode so errors/logs show in standard terminal)
    let plan_context = build_plan(&config_path, exact_filter, env.clone()).await?;

    // Initialize Terminal Guard (Restores terminal on drop)
    let mut guard = TerminalGuard::init()?;

    // Setup Communication Channels
    let channels = setup_channels();

    // Start Background Tasks
    let event_bus = EventBus::new();
    spawn_background_tasks(
        event_bus,
        plan_context.core_plan,
        plan_context.dag,
        channels.event_tx,
        channels.command_rx,
        cancel,
        env,
    );

    // Run Application
    let mut app = App::new(
        channels.event_rx,
        channels.command_tx,
        channels.terminal_rx,
        plan_context.pipelines,
        plan_context.report,
    );

    app.run(guard.terminal())
        .await
        .map_err(|e| CliError::Unknown(e.to_string()))?;

    Ok(())
}

/// Builds execution plan with animated spinner feedback
async fn build_plan(
    config_path: &str,
    exact_filter: bool,
    env: Arc<EnvContext>,
) -> Result<crate::tui::plan::PlanContext, CliError> {
    let spinner = ProgressBar::new_spinner();
    spinner.set_style(
        ProgressStyle::default_spinner()
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
            .template("{spinner:.cyan} {msg}")
            .unwrap(),
    );

    let msg = if exact_filter {
        format!(
            "Building execution plan from {} (using exact COUNT - this may take longer)",
            config_path
        )
    } else {
        format!("Building execution plan from {}", config_path)
    };
    spinner.set_message(msg);
    spinner.enable_steady_tick(std::time::Duration::from_millis(80));

    let plan_context = build_plan_context(config_path, exact_filter, env).await?;

    // Clear the spinner before TUI takes over
    spinner.finish_and_clear();

    Ok(plan_context)
}

/// Communication channels between TUI components
struct Channels {
    event_tx: mpsc::Sender<MigrationEvent>,
    event_rx: mpsc::Receiver<MigrationEvent>,
    command_tx: mpsc::Sender<MigrationCommand>,
    command_rx: mpsc::Receiver<MigrationCommand>,
    terminal_rx: mpsc::Receiver<TerminalEvent>,
}

/// Sets up all communication channels
fn setup_channels() -> Channels {
    let (event_tx, event_rx) = mpsc::channel(1000);
    let (command_tx, command_rx) = mpsc::channel(100);
    let terminal_rx = spawn_terminal_events();

    Channels {
        event_tx,
        event_rx,
        command_tx,
        command_rx,
        terminal_rx,
    }
}

/// Spawns all background tasks
fn spawn_background_tasks(
    event_bus: EventBus,
    core_plan: ExecutionPlan,
    dag: Dag,
    event_tx: mpsc::Sender<MigrationEvent>,
    command_rx: mpsc::Receiver<MigrationCommand>,
    cancel: CancellationToken,
    env: Arc<EnvContext>,
) {
    spawn_event_forwarder(event_bus.clone(), event_tx);
    spawn_command_handler(command_rx, cancel.clone());
    spawn_executor(event_bus, core_plan, dag, cancel, env);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_channels_setup() {
        // Verify channels can be created
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let _channels = setup_channels();
            // If we got here, channels were created successfully
        });
    }
}
