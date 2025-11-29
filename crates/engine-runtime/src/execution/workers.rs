use crate::{actor::coordinator::PipelineCoordinator, error::MigrationError};
use engine_config::report::dry_run::DryRunReport;
use engine_core::{context::item::ItemContext, event_bus::bus::EventBus, metrics::Metrics};
use engine_processing::{consumer::create_consumer, producer::create_producer};
use futures::lock::Mutex;
use model::{events::migration::MigrationEvent, records::batch::Batch};
use smql_syntax::ast::setting::Settings;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

pub async fn spawn(
    ctx: Arc<Mutex<ItemContext>>,
    settings: &Settings,
    cancel: CancellationToken,
    report: &Arc<Mutex<DryRunReport>>,
) -> Result<(), MigrationError> {
    info!("Launching workers");

    let (batch_tx, batch_rx) = mpsc::channel::<Batch>(64);
    let metrics = Metrics::new();

    let producer = create_producer(&ctx, batch_tx, settings, report).await;
    let consumer = create_consumer(&ctx, batch_rx, cancel.clone(), metrics.clone()).await;

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

    // Wait for pipeline to complete
    coordinator.wait().await.map_err(|e| {
        error!("Pipeline error: {}", e);
        MigrationError::Unexpected(format!("Pipeline error: {}", e))
    })?;

    info!("Pipeline completed successfully");
    Ok(())
}

/// Subscribes to migration events for logging and monitoring.
async fn subscribe_to_events(event_bus: EventBus) {
    // Create a single channel for all migration events
    let (event_tx, mut event_rx) = mpsc::channel::<Arc<MigrationEvent>>(100);

    // Subscribe to MigrationEvent
    event_bus.subscribe::<MigrationEvent>(event_tx).await;

    // Spawn background task to handle all migration events
    tokio::spawn(async move {
        while let Some(event) = event_rx.recv().await {
            match event.as_ref() {
                MigrationEvent::ProducerStarted {
                    run_id,
                    item_id,
                    mode,
                    ..
                } => {
                    info!(
                        run_id = %run_id,
                        item_id = %item_id,
                        mode = ?mode,
                        "Producer started"
                    );
                }

                MigrationEvent::ProducerStopped {
                    run_id,
                    item_id,
                    rows_produced,
                    ..
                } => {
                    info!(
                        run_id = %run_id,
                        item_id = %item_id,
                        rows_produced = rows_produced,
                        "Producer stopped"
                    );
                }

                MigrationEvent::ConsumerStarted {
                    run_id,
                    item_id,
                    part_id,
                    ..
                } => {
                    info!(
                        run_id = %run_id,
                        item_id = %item_id,
                        part_id = %part_id,
                        "Consumer started"
                    );
                }

                MigrationEvent::ConsumerStopped {
                    run_id,
                    item_id,
                    part_id,
                    rows_written,
                    ..
                } => {
                    info!(
                        run_id = %run_id,
                        item_id = %item_id,
                        part_id = %part_id,
                        rows_written = rows_written,
                        "Consumer stopped"
                    );
                }

                MigrationEvent::SnapshotStarted {
                    run_id,
                    item_id,
                    estimated_rows,
                    ..
                } => {
                    info!(
                        run_id = %run_id,
                        item_id = %item_id,
                        estimated_rows = ?estimated_rows,
                        "Snapshot phase started"
                    );
                }

                MigrationEvent::SnapshotCompleted {
                    run_id,
                    item_id,
                    rows_processed,
                    duration_ms,
                    ..
                } => {
                    info!(
                        run_id = %run_id,
                        item_id = %item_id,
                        rows_processed = rows_processed,
                        duration_ms = duration_ms,
                        "Snapshot phase completed"
                    );
                }

                MigrationEvent::CdcStarted {
                    run_id,
                    item_id,
                    starting_position,
                    ..
                } => {
                    info!(
                        run_id = %run_id,
                        item_id = %item_id,
                        starting_position = ?starting_position,
                        "CDC phase started"
                    );
                }

                MigrationEvent::CdcStopped {
                    run_id,
                    item_id,
                    final_position,
                    ..
                } => {
                    info!(
                        run_id = %run_id,
                        item_id = %item_id,
                        final_position = ?final_position,
                        "CDC phase stopped"
                    );
                }

                MigrationEvent::BatchProcessed {
                    run_id,
                    item_id,
                    batch_id,
                    row_count,
                    ..
                } => {
                    debug!(
                        run_id = %run_id,
                        item_id = %item_id,
                        batch_id = %batch_id,
                        row_count = row_count,
                        "Batch processed"
                    );
                }

                MigrationEvent::Progress {
                    run_id,
                    item_id,
                    rows_processed,
                    rows_per_second,
                    percentage,
                    eta_seconds,
                    ..
                } => {
                    info!(
                        run_id = %run_id,
                        item_id = %item_id,
                        rows_processed = rows_processed,
                        rows_per_second = rows_per_second,
                        percentage = ?percentage,
                        eta_seconds = ?eta_seconds,
                        "Migration progress"
                    );
                }

                MigrationEvent::Failed {
                    run_id,
                    item_id,
                    error,
                    rows_processed,
                    ..
                } => {
                    error!(
                        run_id = %run_id,
                        item_id = %item_id,
                        error = %error,
                        rows_processed = rows_processed,
                        "Migration failed"
                    );
                }

                MigrationEvent::ActorError {
                    actor_name,
                    run_id,
                    item_id,
                    error,
                    recoverable,
                    ..
                } => {
                    if *recoverable {
                        info!(
                            actor = %actor_name,
                            run_id = ?run_id,
                            item_id = ?item_id,
                            error = %error,
                            "Recoverable actor error"
                        );
                    } else {
                        error!(
                            actor = %actor_name,
                            run_id = ?run_id,
                            item_id = ?item_id,
                            error = %error,
                            "Fatal actor error"
                        );
                    }
                }

                _ => {
                    // Log other events at debug level
                    debug!(event_type = event.event_type(), "{}", event);
                }
            }
        }
    });

    info!("Event subscriber configured for migration monitoring");
}
