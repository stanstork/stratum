use crate::{
    actor::coordinator::PipelineCoordinator,
    dag::endpoint::{DestinationEndpoint, HookPhase},
    error::MigrationError,
};
use chrono;
use connectors::sql::metadata::table::TableMetadata;
use engine_config::settings::validated::ValidatedSettings;
use engine_core::{event_bus::bus::EventBus, metrics::Metrics, schema::schema_ops::SchemaOps};
use engine_infra::shutdown::ShutdownSignal;
use engine_processing::{
    consumer::Consumer,
    context::PipelineContext,
    producer::{Producer, config::ProducerConfig},
};
use model::integrity::{algorithm::HashAlgorithm, config::IntegrityConfig};
use model::{
    events::migration::MigrationEvent,
    execution::{pipeline::Pipeline, references::DataMode},
    records::batch::Batch,
};
use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

const BATCH_CHANNEL_CAPACITY: usize = 64;
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

/// Orchestrates the complete pipeline execution lifecycle including hooks.
/// The orchestrator ensures proper sequencing and error handling across all phases.
pub struct PipelineOrchestrator {
    pipeline: Pipeline,
    ctx: PipelineContext,
    dest_ep: Box<dyn DestinationEndpoint>,
    settings: ValidatedSettings,
    schema_ops: SchemaOps,
    shutdown: ShutdownSignal,
    event_bus: EventBus,
    done_ops: Arc<Mutex<HashSet<String>>>,
    cascade_tables: Vec<String>,
}

impl PipelineOrchestrator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        pipeline: Pipeline,
        ctx: PipelineContext,
        dest_ep: Box<dyn DestinationEndpoint>,
        settings: ValidatedSettings,
        schema_ops: SchemaOps,
        shutdown: ShutdownSignal,
        event_bus: EventBus,
        done_ops: Arc<Mutex<HashSet<String>>>,
        cascade_tables: Vec<String>,
    ) -> Self {
        Self {
            pipeline,
            ctx,
            dest_ep,
            settings,
            schema_ops,
            shutdown,
            event_bus,
            done_ops,
            cascade_tables,
        }
    }

    /// Executes the complete pipeline lifecycle:
    /// pre-DDL -> before hooks -> data migration -> post-DDL -> after hooks
    /// Returns the number of rows processed.
    pub async fn execute(&self) -> Result<u64, MigrationError> {
        self.execute_schema_ops("pre-migration", &self.schema_ops.pre)
            .await?;
        self.execute_hooks(HookPhase::Before).await?;

        let rows = if self.is_schema_only() {
            info!(
                "Schema-only mode: skipping data migration for pipeline '{}'",
                self.pipeline.name
            );
            0
        } else {
            self.execute_pipeline().await?
        };

        self.execute_schema_ops("post-migration", &self.schema_ops.post)
            .await?;
        self.execute_hooks(HookPhase::After).await?;
        Ok(rows)
    }

    /// Execute a batch of schema operations against the destination endpoint,
    /// skipping any ops whose SQL has already been executed in a prior pipeline.
    async fn execute_schema_ops(
        &self,
        phase: &str,
        ops: &[engine_core::schema::schema_ops::SchemaOp],
    ) -> Result<(), MigrationError> {
        if ops.is_empty() {
            return Ok(());
        }

        info!("Executing {} {} schema operations", ops.len(), phase);

        for op in ops {
            {
                let set = self.done_ops.lock().unwrap();
                if set.contains(&op.sql) {
                    info!("Skipping already-executed schema op: {}", op.description);
                    continue;
                }
            }

            self.dest_ep
                .apply_schema_ops(std::slice::from_ref(op), phase)
                .await?;

            self.done_ops.lock().unwrap().insert(op.sql.clone());
        }

        Ok(())
    }

    fn is_schema_only(&self) -> bool {
        self.pipeline
            .source
            .graph_references
            .as_ref()
            .is_some_and(|r| matches!(r.data_mode, DataMode::SchemaOnly))
    }

    async fn execute_hooks(&self, phase: HookPhase) -> Result<(), MigrationError> {
        let hooks = match &self.pipeline.lifecycle {
            Some(h) => h,
            None => return Ok(()),
        };

        // Check if there are actual hooks to run for this phase
        let should_run = match phase {
            HookPhase::Before => !hooks.before.is_empty(),
            HookPhase::After => !hooks.after.is_empty(),
        };

        if should_run {
            self.dest_ep.run_hooks(phase, hooks).await?;
        }

        Ok(())
    }

    /// Returns the number of rows processed.
    async fn execute_pipeline(&self) -> Result<u64, MigrationError> {
        info!("Starting pipeline execution: {}", self.pipeline.name);

        self.publish_started().await;
        let start_time = std::time::Instant::now();

        // Initialize Producer, Consumer, and Coordinator
        let (coordinator, metrics) = self.build_coordinator().await?;

        // Run the pipeline with cancellation support
        self.await_completion_or_cancel(coordinator, &metrics, start_time)
            .await?;

        Ok(metrics.snapshot().records_processed)
    }

    async fn build_coordinator(&self) -> Result<(PipelineCoordinator, Metrics), MigrationError> {
        let (batch_tx, batch_rx) = mpsc::channel::<Batch>(BATCH_CHANNEL_CAPACITY);
        let metrics = Metrics::new();

        let dest_metas = self.fetch_destination_metadata().await?;
        let config = self.build_producer_config(&dest_metas);

        let producer = Producer::new(
            &self.ctx,
            batch_tx,
            config,
            self.settings.mapped_columns_only(),
        )
        .await
        .map_err(|e| MigrationError::InitializationError(e.to_string()))?;

        let consumer = Consumer::new(
            &self.ctx,
            batch_rx,
            dest_metas,
            self.shutdown.clone(),
            metrics.clone(),
        )
        .await;

        let coordinator = PipelineCoordinator::new(
            producer,
            consumer,
            metrics.clone(),
            self.shutdown.cancel.clone(),
            self.event_bus.clone(),
        );

        Ok((coordinator, metrics))
    }

    /// Fetches destination table metadata.
    /// In cascade mode, fetches metadata for all discovered tables.
    /// Otherwise, just the single destination table.
    async fn fetch_destination_metadata(&self) -> Result<Vec<TableMetadata>, MigrationError> {
        self.dest_ep
            .destination_metadata(&self.ctx, &self.cascade_tables)
            .await
    }

    fn build_producer_config(&self, dest_metas: &[TableMetadata]) -> ProducerConfig {
        let mut config = ProducerConfig::default().with_batch_size(self.settings.batch_size);

        if self.settings.integrity().is_enabled() {
            if self.pipeline.source.pagination.is_none() {
                warn!(
                    "Pipeline '{}' has integrity enabled but no `paginate` block. \
                     Without explicit pagination the row order is non-deterministic, \
                     which may cause `verify` to report false mismatches. \
                     Add a `paginate` block for reliable verification.",
                    self.pipeline.name
                );
            }

            let tables = dest_metas
                .iter()
                .map(|m| (m.name.clone(), m.columns.keys().cloned().collect()))
                .collect();

            let column_types = dest_metas
                .iter()
                .map(|m| {
                    let col_types = m
                        .columns
                        .values()
                        .map(|c| (c.name.clone(), c.data_type.clone()))
                        .collect();
                    (m.name.clone(), col_types)
                })
                .collect();

            let integrity =
                IntegrityConfig::new(HashAlgorithm::Sha256, tables, &self.ctx.destination.name)
                    .with_column_types(column_types)
                    .with_store_row_hashes(self.settings.integrity().store_row_hashes());

            config = config.with_integrity(integrity);
        }

        config
    }

    async fn await_completion_or_cancel(
        &self,
        coordinator: PipelineCoordinator,
        metrics: &Metrics,
        start_time: std::time::Instant,
    ) -> Result<(), MigrationError> {
        let part_id = "part-0".to_string(); // TODO: Make this dynamic when multi-part support is added

        coordinator
            .start_snapshot_pipeline(self.ctx.run_id.clone(), self.ctx.item_id.clone(), part_id)
            .await
            .map_err(|e| {
                error!("Failed to start snapshot pipeline: {}", e);
                MigrationError::PipelineFailed(format!("Failed to start pipeline: {}", e))
            })?;

        let cancel_fut = self.shutdown.cancel.cancelled();
        let pause_fut = self.shutdown.pause.cancelled();
        let wait_fut = coordinator.wait();

        tokio::pin!(cancel_fut);
        tokio::pin!(pause_fut);
        tokio::pin!(wait_fut);

        tokio::select! {
            result = &mut wait_fut => {
                self.handle_pipeline_result(result, metrics, start_time).await
            }
            _ = &mut pause_fut => {
                self.handle_pause(wait_fut).await
            }
            _ = &mut cancel_fut => {
                self.handle_shutdown(wait_fut).await
            }
        }
    }

    async fn handle_pipeline_result(
        &self,
        result: Result<(), impl std::fmt::Display>,
        metrics: &Metrics,
        start_time: std::time::Instant,
    ) -> Result<(), MigrationError> {
        match result {
            Ok(()) => {
                info!("Pipeline completed successfully");
                self.publish_completed(metrics, start_time).await;
                Ok(())
            }
            Err(e) => {
                error!("Pipeline error: {}", e);
                self.publish_failed(&e.to_string(), metrics).await;
                Err(MigrationError::PipelineFailed(format!(
                    "Pipeline error: {}",
                    e
                )))
            }
        }
    }

    async fn handle_pause(
        &self,
        wait_fut: impl Future<Output = Result<(), impl std::fmt::Display>>,
    ) -> Result<(), MigrationError> {
        info!("Pause signal received - draining current batch");

        // Trigger cancel so producer/consumer finish current work and stop
        self.shutdown.cancel.cancel();

        match tokio::time::timeout(SHUTDOWN_TIMEOUT, wait_fut).await {
            Ok(Ok(())) => {
                info!("Pipeline paused gracefully after draining batch");
                Err(MigrationError::Paused)
            }
            Ok(Err(e)) => {
                error!("Pipeline error during pause drain: {}", e);
                Err(MigrationError::PipelineFailed(format!(
                    "Pipeline error during pause: {}",
                    e
                )))
            }
            Err(_) => {
                warn!(
                    "Pipeline did not drain within {}s timeout - progress has been checkpointed",
                    SHUTDOWN_TIMEOUT.as_secs()
                );
                Err(MigrationError::Paused)
            }
        }
    }

    async fn handle_shutdown(
        &self,
        wait_fut: impl Future<Output = Result<(), impl std::fmt::Display>>,
    ) -> Result<(), MigrationError> {
        warn!("Shutdown signal received, waiting for in-flight operations to complete");
        info!(
            "Waiting up to {}s for graceful shutdown",
            SHUTDOWN_TIMEOUT.as_secs()
        );

        match tokio::time::timeout(SHUTDOWN_TIMEOUT, wait_fut).await {
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

    async fn publish_started(&self) {
        self.event_bus
            .publish(MigrationEvent::Started {
                run_id: self.ctx.run_id.clone(),
                item_id: self.ctx.item_id.clone(),
                source: self.pipeline.source.connection.name.clone(),
                destination: self.pipeline.destination.connection.name.clone(),
                timestamp: chrono::Utc::now(),
            })
            .await;
    }

    async fn publish_completed(&self, metrics: &Metrics, start_time: std::time::Instant) {
        let snapshot = metrics.snapshot();
        self.event_bus
            .publish(MigrationEvent::Completed {
                run_id: self.ctx.run_id.clone(),
                item_id: self.ctx.item_id.clone(),
                rows_processed: snapshot.records_processed,
                rows_skipped: snapshot.rows_skipped,
                rows_failed: snapshot.rows_failed,
                duration_ms: start_time.elapsed().as_millis() as u64,
                timestamp: chrono::Utc::now(),
            })
            .await;
    }

    async fn publish_failed(&self, error: &str, metrics: &Metrics) {
        let snapshot = metrics.snapshot();
        self.event_bus
            .publish(MigrationEvent::Failed {
                run_id: self.ctx.run_id.clone(),
                item_id: self.ctx.item_id.clone(),
                error: error.to_string(),
                error_code: None,
                rows_processed: snapshot.records_processed,
                timestamp: chrono::Utc::now(),
            })
            .await;
    }
}
