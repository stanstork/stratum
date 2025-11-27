use crate::{actor::coordinator::PipelineCoordinator, error::MigrationError};
use engine_config::report::dry_run::DryRunReport;
use engine_core::{context::item::ItemContext, metrics::Metrics};
use engine_processing::{consumer::create_consumer, producer::create_producer};
use futures::lock::Mutex;
use model::records::batch::Batch;
use smql_syntax::ast::setting::Settings;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

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

    // Get run_id and item_id from context
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
