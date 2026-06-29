use crate::context::PipelineContext;
use crate::{
    consumer::components::{coordinator::BatchCoordinator, writer::BatchWriter},
    error::ConsumerError,
    item::ItemId,
    state_manager::StateManager,
};
use connectors::sql::metadata::table::TableMetadata;
use engine_core::{metrics::Metrics, retry::RetryPolicy};
use engine_infra::shutdown::ShutdownSignal;
use engine_state::models::CheckpointStage;
use model::records::batch::Batch;
use tokio::sync::mpsc;
use tracing::{debug, error, warn};

pub mod components;
pub mod config;

#[derive(Clone, Debug, PartialEq)]
pub enum ConsumerStatus {
    /// Work is ongoing; the actor should schedule another tick immediately.
    Working,
    /// The consumer is idle (waiting for batches).
    Idle,
    /// The consumer has finished (channel closed, all work done).
    Finished,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConsumerMode {
    /// Consumer is idle, waiting to start.
    Idle,

    /// Consumer is actively processing batches.
    Running,

    /// Consumer is flushing pending writes before shutdown.
    Flushing,

    /// Consumer has finished all work.
    Finished,
}

pub struct Consumer {
    // Components
    coordinator: BatchCoordinator,

    // Communication
    shutdown: ShutdownSignal,

    // State
    mode: ConsumerMode,
    ids: ItemId,
}

impl Consumer {
    pub async fn new(
        ctx: &PipelineContext,
        batch_rx: mpsc::Receiver<Batch>,
        dest_metadata: Vec<TableMetadata>,
        shutdown: ShutdownSignal,
        metrics: Metrics,
    ) -> Self {
        let run_id = ctx.run_id.clone();
        let item_id = ctx.item_id.clone();
        let destination = ctx.destination.clone();
        let pipeline = ctx.pipeline.clone();
        let state_store = ctx.state.clone();

        let part_id = "part-0".to_string();
        let ids = ItemId::new(run_id, item_id, part_id);

        let meta = dest_metadata;

        // Create retry policy from pipeline config, fallback to database defaults
        let retry_config = pipeline
            .error_handling
            .as_ref()
            .and_then(|eh| eh.retry.as_ref());
        let retry_policy = RetryPolicy::from_config(retry_config);

        let writer = BatchWriter::new(destination.clone(), retry_policy, &meta)
            .auto_detect_strategy() // Detects fast path (COPY/MERGE) availability
            .await;
        let state_manager = StateManager::new(ids.clone(), state_store);
        let coordinator = BatchCoordinator::new(writer, state_manager, metrics.clone(), batch_rx);

        Self {
            coordinator,
            shutdown,
            mode: ConsumerMode::Idle,
            ids,
        }
    }

    pub async fn start(&mut self) -> Result<(), ConsumerError> {
        debug!(
            run_id = %self.ids.run_id(),
            item_id = %self.ids.item_id(),
            "starting consumer"
        );

        // Sink one-time setup before any batch is written.
        self.coordinator.prepare().await?;

        self.mode = ConsumerMode::Running;
        debug!("consumer started");
        Ok(())
    }

    pub async fn finalize(&mut self) -> Result<(), ConsumerError> {
        self.coordinator.finalize().await
    }

    pub async fn resume(
        &mut self,
        run_id: &str,
        item_id: &str,
        part_id: &str,
    ) -> Result<(), ConsumerError> {
        debug!(
            run_id = run_id,
            item_id = item_id,
            part_id = part_id,
            "resuming consumer from checkpoint"
        );

        // Load last checkpoint to verify state
        match self.coordinator.load_last_checkpoint().await? {
            Some(checkpoint) => {
                debug!(
                    stage = %checkpoint.stage,
                    rows_done = checkpoint.rows_done,
                    cursor = ?checkpoint.src_offset,
                    "loaded checkpoint, continuing from last position"
                );

                // If we crashed during "write" stage, the producer will re-send
                // that batch based on its checkpoint recovery logic
                if checkpoint.stage == CheckpointStage::Write {
                    warn!(
                        batch_id = %checkpoint.batch_id,
                        "last batch was mid-write at crash, producer may re-send it"
                    );
                }
            }
            None => {
                debug!("no checkpoint found, consumer starting fresh");
            }
        }

        self.mode = ConsumerMode::Running;
        Ok(())
    }

    pub async fn tick(&mut self) -> Result<ConsumerStatus, ConsumerError> {
        match self.mode {
            ConsumerMode::Idle => {
                // Not yet started
                Ok(ConsumerStatus::Idle)
            }

            ConsumerMode::Finished => {
                // Already finished, nothing to do
                Ok(ConsumerStatus::Finished)
            }

            ConsumerMode::Running => {
                if self.should_stop() {
                    debug!("stop signal received, entering flush mode");
                    self.mode = ConsumerMode::Flushing;
                    return Ok(ConsumerStatus::Working); // Continue to flush
                }

                match self.coordinator.try_process_one().await {
                    Ok(true) => {
                        // Successfully processed a batch
                        // More batches may be available, return Working
                        Ok(ConsumerStatus::Working)
                    }
                    Ok(false) => {
                        // No batch available right now
                        // Check if channel is closed (producer finished)
                        if self.coordinator.is_channel_closed() {
                            debug!("batch channel closed and drained, consumer finished");
                            self.mode = ConsumerMode::Finished;
                            return Ok(ConsumerStatus::Finished);
                        }
                        // Channel still open, just idle
                        Ok(ConsumerStatus::Idle)
                    }
                    Err(e) => {
                        error!(error = %e, "failed to process batch");
                        // On error, enter flushing mode to clean up
                        self.mode = ConsumerMode::Flushing;
                        Err(e)
                    }
                }
            }

            ConsumerMode::Flushing => {
                // Flush any pending writes before finishing
                debug!("flushing consumer writes");

                if let Err(e) = self.coordinator.try_process_one().await {
                    error!(error = %e, "failed to flush on shutdown");
                }

                self.mode = ConsumerMode::Finished;
                Ok(ConsumerStatus::Finished)
            }
        }
    }

    pub async fn stop(&mut self) -> Result<(), ConsumerError> {
        debug!(
            run_id = %self.ids.run_id(),
            item_id = %self.ids.item_id(),
            "stopping consumer"
        );

        self.mode = ConsumerMode::Finished;
        debug!("consumer stopped");
        Ok(())
    }

    pub fn rows_written(&self) -> u64 {
        self.coordinator.rows_processed()
    }

    /// Check if we should stop processing.
    fn should_stop(&self) -> bool {
        self.shutdown.cancel.is_cancelled()
    }
}
