use crate::tui::app::command::MigrationCommand;
use engine_core::{context::env::EnvContext, plan::execution::ExecutionPlan as CoreExecutionPlan};
use engine_infra::event_bus::bus::EventBus;
use engine_infra::shutdown::ShutdownSignal;
use engine_runtime::dag::{Dag, executor::DagExecutor};
use engine_runtime::error::MigrationError;
use model::{events::migration::MigrationEvent, execution::flags::ExecutionFlags};
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc;
use tracing::{error, info};

/// Forwards events from the engine's global EventBus to the TUI's local receiver
pub fn spawn_event_forwarder(event_bus: EventBus, tui_tx: mpsc::Sender<MigrationEvent>) {
    tokio::spawn(async move {
        let (bus_tx, mut bus_rx) = mpsc::channel(100);
        event_bus.subscribe::<MigrationEvent>(bus_tx).await;

        while let Some(event) = bus_rx.recv().await {
            if tui_tx.send((*event).clone()).await.is_err() {
                break; // TUI closed
            }
        }
    });
}

/// Bridges TUI commands to the engine via the shared shutdown signal.
pub fn spawn_command_handler(
    mut command_rx: mpsc::Receiver<MigrationCommand>,
    shutdown: ShutdownSignal,
) {
    tokio::spawn(async move {
        while let Some(cmd) = command_rx.recv().await {
            match cmd {
                MigrationCommand::PauseAll => {
                    info!("pause & checkpoint requested from TUI");
                    shutdown.pause.cancel();
                }
                MigrationCommand::CancelAll => {
                    info!("cancel requested from TUI");
                    shutdown.cancel.cancel();
                    break;
                }
                other => {
                    info!(command = ?other, "command not supported by the engine yet; ignoring");
                }
            }
        }
    });
}

/// How the background migration task ended, reported to the TUI so it can show
/// the right terminal state.
#[derive(Debug, Clone)]
pub enum ExecOutcome {
    /// The whole run finished.
    Completed,
    /// User asked to pause; the run drained and checkpointed.
    Paused,
    /// User asked to cancel; the run was aborted.
    Cancelled,
    /// The engine failed for a reason with no per-pipeline event.
    Failed(String),
}

/// Manages the background execution of the migration DAG.
pub fn spawn_executor(
    flags: ExecutionFlags,
    bus: EventBus,
    plan: CoreExecutionPlan,
    graph: Dag,
    shutdown: ShutdownSignal,
    env: Arc<EnvContext>,
    outcome_tx: mpsc::Sender<ExecOutcome>,
) {
    tokio::spawn(async move {
        // Debounce start to let TUI paint first frame
        tokio::time::sleep(Duration::from_millis(500)).await;

        let result = DagExecutor::with_event_bus(plan, flags, shutdown, bus, env).await;

        match result {
            Ok(executor) => match executor.execute(graph).await {
                Ok(()) => {
                    let _ = outcome_tx.send(ExecOutcome::Completed).await;
                }
                Err(e) => {
                    let outcome = match e {
                        // User-initiated stops (Space=pause, c=cancel) surface as
                        // errors here but are not failures.
                        MigrationError::Paused => {
                            info!("migration paused by user");
                            ExecOutcome::Paused
                        }
                        MigrationError::ShutdownRequested => {
                            info!("migration cancelled by user");
                            ExecOutcome::Cancelled
                        }
                        other => {
                            error!(error = %other, "migration execution failed");
                            ExecOutcome::Failed(format!("Migration execution failed: {other}"))
                        }
                    };
                    let _ = outcome_tx.send(outcome).await;
                }
            },
            Err(e) => {
                error!(error = %e, "failed to initialize executor");
                let _ = outcome_tx
                    .send(ExecOutcome::Failed(format!(
                        "Failed to initialize the engine: {e}"
                    )))
                    .await;
            }
        }
    });
}
