use crate::{actor::coordinator::PipelineCoordinator, error::MigrationError};
use engine_config::report::dry_run::DryRunReport;
use engine_core::{context::item::ItemContext, event_bus::bus::EventBus, metrics::Metrics};
use engine_processing::{consumer::create_consumer, producer::create_producer};
use futures::lock::Mutex;
use model::{events::*, records::batch::Batch};
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
    // Create channels for different event types
    let (producer_tx, mut producer_rx) = mpsc::channel::<Arc<ProducerStarted>>(32);
    let (consumer_tx, mut consumer_rx) = mpsc::channel::<Arc<ConsumerStarted>>(32);
    let (snapshot_tx, mut snapshot_rx) = mpsc::channel::<Arc<SnapshotStarted>>(32);
    let (cdc_tx, mut cdc_rx) = mpsc::channel::<Arc<CdcStarted>>(32);
    let (batch_tx, mut batch_rx) = mpsc::channel::<Arc<BatchProcessed>>(32);
    let (progress_tx, mut progress_rx) = mpsc::channel::<Arc<MigrationProgress>>(32);

    event_bus.subscribe::<ProducerStarted>(producer_tx).await;
    event_bus.subscribe::<ConsumerStarted>(consumer_tx).await;
    event_bus.subscribe::<SnapshotStarted>(snapshot_tx).await;
    event_bus.subscribe::<CdcStarted>(cdc_tx).await;
    event_bus.subscribe::<BatchProcessed>(batch_tx).await;
    event_bus.subscribe::<MigrationProgress>(progress_tx).await;

    // Spawn background task to handle producer events
    tokio::spawn(async move {
        while let Some(event) = producer_rx.recv().await {
            info!(
                run_id = %event.run_id,
                item_id = %event.item_id,
                mode = ?event.mode,
                "Producer started"
            );
        }
    });

    // Spawn background task to handle consumer events
    tokio::spawn(async move {
        while let Some(event) = consumer_rx.recv().await {
            info!(
                run_id = %event.run_id,
                item_id = %event.item_id,
                "Consumer started"
            );
        }
    });

    // Spawn background task to handle snapshot events
    tokio::spawn(async move {
        while let Some(event) = snapshot_rx.recv().await {
            info!(
                run_id = %event.run_id,
                item_id = %event.item_id,
                "Snapshot phase started"
            );
        }
    });

    // Spawn background task to handle CDC events
    tokio::spawn(async move {
        while let Some(event) = cdc_rx.recv().await {
            info!(
                run_id = %event.run_id,
                item_id = %event.item_id,
                "CDC phase started"
            );
        }
    });

    // Spawn background task to handle batch processing events
    tokio::spawn(async move {
        while let Some(event) = batch_rx.recv().await {
            debug!(
                run_id = %event.run_id,
                item_id = %event.item_id,
                batch_id = %event.batch_id,
                row_count = event.row_count,
                "Batch processed"
            );
        }
    });

    // Spawn background task to handle progress events
    tokio::spawn(async move {
        while let Some(event) = progress_rx.recv().await {
            info!(
                run_id = %event.run_id,
                item_id = %event.item_id,
                rows_processed = event.rows_processed,
                percentage = ?event.percentage,
                "Migration progress"
            );
        }
    });

    info!("Event subscribers configured for migration monitoring");
}
