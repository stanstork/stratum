use crate::{actor::coordinator::PipelineCoordinator, error::MigrationError};
use chrono;
use connectors::adapter::Adapter;
use engine_config::settings::validated::ValidatedSettings;
use engine_core::{context::item::ItemContext, event_bus::bus::EventBus, metrics::Metrics};
use engine_processing::{consumer::Consumer, hooks::executor::HookExecutor, producer::Producer};
use futures::lock::Mutex;
use model::{
    events::migration::MigrationEvent, execution::pipeline::Pipeline, records::batch::Batch,
};
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

/// Orchestrates the complete pipeline execution lifecycle including hooks.
/// The orchestrator ensures proper sequencing and error handling across all phases.
pub struct PipelineOrchestrator {
    pipeline: Pipeline,
    ctx: Arc<Mutex<ItemContext>>,
    settings: ValidatedSettings,
    cancel: CancellationToken,
    event_bus: EventBus,
}

impl PipelineOrchestrator {
    pub fn new(
        pipeline: Pipeline,
        ctx: Arc<Mutex<ItemContext>>,
        settings: ValidatedSettings,
        cancel: CancellationToken,
        event_bus: EventBus,
    ) -> Self {
        Self {
            pipeline,
            ctx,
            settings,
            cancel,
            event_bus,
        }
    }

    /// Executes the complete pipeline lifecycle with hooks.
    pub async fn execute(&self) -> Result<(), MigrationError> {
        // Execute before hooks
        self.execute_before_hooks().await?;

        // Execute the data migration pipeline
        self.execute_pipeline().await?;

        // Execute after hooks (only on success)
        self.execute_after_hooks().await?;

        Ok(())
    }

    async fn execute_before_hooks(&self) -> Result<(), MigrationError> {
        if let Some(ref hooks) = self.pipeline.lifecycle
            && !hooks.before.is_empty()
        {
            let adapter = self.get_adapter().await?;
            let mut executor = HookExecutor::new(adapter, hooks.clone());
            executor.execute_before().await.map_err(|e| {
                MigrationError::HookExecutionFailed(format!("Before hooks failed: {}", e))
            })?;
        }
        Ok(())
    }

    /// Executes the main data migration pipeline.
    async fn execute_pipeline(&self) -> Result<(), MigrationError> {
        info!("Starting pipeline execution: {}", self.pipeline.name);

        let (run_id, item_id) = {
            let ctx_guard = self.ctx.lock().await;
            (ctx_guard.run_id.clone(), ctx_guard.item_id.clone())
        };

        let source = self.pipeline.source.connection.name.clone();
        let destination = self.pipeline.destination.connection.name.clone();

        // Publish Started event
        self.event_bus
            .publish(MigrationEvent::Started {
                run_id: run_id.clone(),
                item_id: item_id.clone(),
                source,
                destination,
                timestamp: chrono::Utc::now(),
            })
            .await;

        // Track start time for duration calculation
        let start_time = std::time::Instant::now();

        // Create communication channel between producer and consumer
        let (batch_tx, batch_rx) = mpsc::channel::<Batch>(64);
        let metrics = Metrics::new();

        // Create producer and consumer
        let producer = Producer::new(&self.ctx, batch_tx, &self.settings).await;
        let consumer =
            Consumer::new(&self.ctx, batch_rx, self.cancel.clone(), metrics.clone()).await;

        let coordinator =
            PipelineCoordinator::new(producer, consumer, metrics.clone(), self.cancel.clone());

        coordinator.initialize().await.map_err(|e| {
            error!("Failed to initialize coordinator: {}", e);
            MigrationError::PipelineFailed(format!("Coordinator initialization failed: {}", e))
        })?;

        coordinator
            .set_event_bus(self.event_bus.clone())
            .await
            .map_err(|e| {
                error!("Failed to set event bus: {}", e);
                MigrationError::PipelineFailed(format!("Failed to set event bus: {}", e))
            })?;

        let (run_id, item_id) = {
            let ctx_guard = self.ctx.lock().await;
            (ctx_guard.run_id.clone(), ctx_guard.item_id.clone())
        };

        let part_id = "part-0".to_string();

        // Start the snapshot pipeline
        coordinator
            .start_snapshot_pipeline(run_id.clone(), item_id.clone(), part_id)
            .await
            .map_err(|e| {
                error!("Failed to start snapshot pipeline: {}", e);
                MigrationError::PipelineFailed(format!("Failed to start pipeline: {}", e))
            })?;

        // Wait for pipeline to complete or shutdown
        const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

        let cancel_fut = self.cancel.cancelled();
        tokio::pin!(cancel_fut);

        let wait_fut = coordinator.wait();
        tokio::pin!(wait_fut);

        let pipeline_result = tokio::select! {
            result = &mut wait_fut => {
                match result {
                    Ok(()) => {
                        info!("Pipeline completed successfully");

                        // Publish Completed event
                        let snapshot = metrics.snapshot();
                            let duration_ms = start_time.elapsed().as_millis() as u64;
                            self.event_bus
                                .publish(MigrationEvent::Completed {
                                    run_id: run_id.clone(),
                                    item_id: item_id.clone(),
                                    rows_processed: snapshot.records_processed,
                                    rows_skipped: snapshot.rows_skipped,
                                    rows_failed: snapshot.rows_failed,
                                    duration_ms,
                                    timestamp: chrono::Utc::now(),
                                })
                                .await;

                        Ok(())
                    }
                    Err(e) => {
                        error!("Pipeline error: {}", e);

                        // Publish Failed event
                        let snapshot = metrics.snapshot();
                        self.event_bus
                            .publish(MigrationEvent::Failed {
                                run_id: run_id.clone(),
                                item_id: item_id.clone(),
                                error: e.to_string(),
                                error_code: None,
                                rows_processed: snapshot.records_processed,
                                timestamp: chrono::Utc::now(),
                            })
                            .await;

                        Err(MigrationError::PipelineFailed(format!("Pipeline error: {}", e)))
                    }
                }
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
                        Err(MigrationError::PipelineFailed(format!(
                            "Pipeline error during shutdown: {}",
                            e
                        )))
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
        };

        pipeline_result
    }

    async fn execute_after_hooks(&self) -> Result<(), MigrationError> {
        if let Some(ref hooks) = self.pipeline.lifecycle
            && !hooks.after.is_empty()
        {
            let adapter = self.get_adapter().await?;
            let mut executor = HookExecutor::new(adapter, hooks.clone());
            executor.execute_after().await.map_err(|e| {
                MigrationError::HookExecutionFailed(format!("After hooks failed: {}", e))
            })?;
        }
        Ok(())
    }

    async fn get_adapter(&self) -> Result<Arc<Adapter>, MigrationError> {
        let ctx_guard = self.ctx.lock().await;
        let adapter = ctx_guard
            .exec_ctx
            .get_adapter(&self.pipeline.destination.connection)
            .await?;
        Ok(Arc::new(adapter))
    }
}
