use crate::tui::app::command::MigrationCommand;
use engine_core::{
    context::env::EnvContext, event_bus::bus::EventBus,
    plan::execution::ExecutionPlan as CoreExecutionPlan,
};
use engine_infra::shutdown::ShutdownSignal;
use engine_runtime::dag::{Dag, executor::DagExecutor};
use model::{events::migration::MigrationEvent, execution::flags::ExecutionFlags};
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
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

/// Bridges TUI commands to the Engine/Executor
pub fn spawn_command_handler(
    mut command_rx: mpsc::Receiver<MigrationCommand>,
    cancel: CancellationToken,
) {
    tokio::spawn(async move {
        while let Some(cmd) = command_rx.recv().await {
            match cmd {
                MigrationCommand::CancelAll => {
                    info!("Shutdown/Cancel requested from TUI");
                    cancel.cancel();
                    break;
                }
                _ => {
                    info!("Forwarding command to executor: {:?}", cmd);
                    // TODO: Implement actual forwarding logic to the running executor instance
                }
            }
        }
    });
}

/// Manages the background execution of the migration DAG
pub fn spawn_executor(
    flags: ExecutionFlags,
    bus: EventBus,
    plan: CoreExecutionPlan,
    graph: Dag,
    shutdown: ShutdownSignal,
    env: Arc<EnvContext>,
) {
    tokio::spawn(async move {
        // Debounce start to let TUI paint first frame
        tokio::time::sleep(Duration::from_millis(500)).await;

        let result = DagExecutor::with_event_bus(plan, flags, shutdown, bus, env).await;

        match result {
            Ok(executor) => {
                if let Err(e) = executor.execute(graph).await {
                    error!("Migration execution failed: {}", e);
                }
            }
            Err(e) => error!("Failed to initialize executor: {}", e),
        }
    });
}
