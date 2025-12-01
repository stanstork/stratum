use crate::{actor::coordinator::PipelineCoordinator, error::MigrationError};
use engine_config::{report::dry_run::DryRunReport, settings::validated::ValidatedSettings};
use engine_core::{context::item::ItemContext, event_bus::bus::EventBus, metrics::Metrics};
use engine_processing::{consumer::create_consumer, producer::create_producer};
use futures::lock::Mutex;
use model::{events::migration::MigrationEvent, records::batch::Batch};
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

pub async fn spawn(
    ctx: Arc<Mutex<ItemContext>>,
    settings: &ValidatedSettings,
    cancel: CancellationToken,
    report: &Arc<Mutex<DryRunReport>>,
) -> Result<(), MigrationError> {
    info!("Launching workers");

    let (batch_tx, batch_rx) = mpsc::channel::<Batch>(64);
    let metrics = Metrics::new();

    let producer = create_producer(&ctx, batch_tx, settings, report).await;
    let consumer = create_consumer(&ctx, batch_rx, settings, cancel.clone(), metrics.clone()).await;

    let coordinator = PipelineCoordinator::new(producer, consumer, metrics.clone(), cancel.clone());

    coordinator.initialize().await.map_err(|e| {
        error!("Failed to initialize coordinator: {}", e);
        MigrationError::Unexpected(format!("Coordinator initialization failed: {}", e))
    })?;

    let event_bus = EventBus::new();
    coordinator
        .set_event_bus(event_bus.clone())
        .await
        .map_err(|e| {
            error!("Failed to set event bus: {}", e);
            MigrationError::Unexpected(format!("Failed to set event bus: {}", e))
        })?;

    // Subscribe to migration events for logging and monitoring
    subscribe_to_events(event_bus).await;

    info!("EventBus configured for migration events");

    let (run_id, item_id) = {
        let ctx_guard = ctx.lock().await;
        (ctx_guard.run_id.clone(), ctx_guard.item_id.clone())
    };

    let part_id = "part-0".to_string();

    // Start the snapshot pipeline
    coordinator
        .start_snapshot_pipeline(run_id, item_id, part_id)
        .await
        .map_err(|e| {
            error!("Failed to start snapshot pipeline: {}", e);
            MigrationError::Unexpected(format!("Failed to start pipeline: {}", e))
        })?;

    // Wait for pipeline to complete or shutdown
    const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

    let cancel_fut = cancel.cancelled();
    tokio::pin!(cancel_fut);

    let wait_fut = coordinator.wait();
    tokio::pin!(wait_fut);

    tokio::select! {
        result = &mut wait_fut => {
            result.map_err(|e| {
                error!("Pipeline error: {}", e);
                MigrationError::Unexpected(format!("Pipeline error: {}", e))
            })?;
            info!("Pipeline completed successfully");
            Ok(())
        }
        _ = &mut cancel_fut => {
            warn!("Shutdown signal received, waiting for in-flight operations to complete");
            info!("Waiting up to {}s for graceful shutdown", SHUTDOWN_TIMEOUT.as_secs());

            // Give the pipeline time to finish in-flight operations
            let shutdown_result = tokio::time::timeout(SHUTDOWN_TIMEOUT, wait_fut).await;

            match shutdown_result {
                Ok(Ok(())) => {
                    info!("Pipeline shutdown completed gracefully");
                    Err(MigrationError::ShutdownRequested)
                }
                Ok(Err(e)) => {
                    error!("Pipeline error during shutdown: {}", e);
                    Err(MigrationError::Unexpected(format!("Pipeline error during shutdown: {}", e)))
                }
                Err(_) => {
                    warn!(
                        "Pipeline did not complete within {}s timeout - progress has been checkpointed",
                        SHUTDOWN_TIMEOUT.as_secs()
                    );
                    Err(MigrationError::ShutdownRequested)
                }
            }
        }
    }
}

/// Subscribes to migration events.
/// Will be used by TUI to monitor migration progress.
async fn subscribe_to_events(event_bus: EventBus) {
    // Create a single channel for all migration events
    let (event_tx, mut event_rx) = mpsc::channel::<Arc<MigrationEvent>>(100);

    event_bus.subscribe::<MigrationEvent>(event_tx).await;

    // Spawn background task to handle all migration events
    tokio::spawn(async move {
        while let Some(event) = event_rx.recv().await {
            info!("Migration Event: {}", event);
        }
    });

    info!("Event subscriber configured for migration monitoring");
}
