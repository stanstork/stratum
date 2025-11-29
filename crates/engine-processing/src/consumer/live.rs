use crate::{
    consumer::{
        ConsumerStatus, DataConsumer,
        components::{coordinator::BatchCoordinator, writer::BatchWriter},
        config::ConsumerConfig,
    },
    error::ConsumerError,
    item::ItemId,
    state_manager::StateManager,
};
use async_trait::async_trait;
use engine_core::{
    connectors::destination::DataDestination, context::item::ItemContext, metrics::Metrics,
    retry::RetryPolicy,
};
use futures::lock::Mutex;
use model::records::batch::Batch;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

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

pub struct LiveConsumer {
    // Components
    coordinator: BatchCoordinator,

    // Communication
    cancel: CancellationToken,

    // State
    mode: ConsumerMode,
    ids: ItemId,

    // Config
    config: ConsumerConfig,
}

impl LiveConsumer {
    pub async fn new(
        ctx: &Arc<Mutex<ItemContext>>,
        batch_rx: mpsc::Receiver<Batch>,
        cancel: CancellationToken,
        metrics: Metrics,
    ) -> Self {
        let (run_id, item_id, destination, state_store) = {
            let c = ctx.lock().await;
            (
                c.run_id.clone(),
                c.item_id.clone(),
                c.destination.clone(),
                c.state.clone(),
            )
        };

        let config = ConsumerConfig::default();
        let part_id = "part-0".to_string();
        let ids = ItemId::new(run_id, item_id, part_id);

        let meta = match &destination.data_dest {
            DataDestination::Database(db) => db.data.lock().await.tables(),
        };

        let writer = BatchWriter::new(destination.clone(), RetryPolicy::for_database(), &meta)
            .auto_detect_strategy() // Detects fast path (COPY/MERGE) availability
            .await;
        let state_manager = StateManager::new(ids.clone(), state_store);
        let coordinator = BatchCoordinator::new(writer, state_manager, metrics.clone(), batch_rx);

        Self {
            coordinator,
            cancel,
            mode: ConsumerMode::Idle,
            ids,
            config,
        }
    }

    /// Check if we should stop processing.
    fn should_stop(&self) -> bool {
        self.cancel.is_cancelled()
    }
}

#[async_trait]
impl DataConsumer for LiveConsumer {
    async fn start(&mut self) -> Result<(), ConsumerError> {
        info!(
            run_id = %self.ids.run_id(),
            item_id = %self.ids.item_id(),
            "Starting LiveConsumer"
        );

        if self.config.disable_triggers {
            info!("Consumer configured to disable triggers on start");
            // TODO: Disable triggers if applicable
        }

        self.mode = ConsumerMode::Running;
        info!("LiveConsumer started successfully");
        Ok(())
    }

    async fn resume(
        &mut self,
        run_id: &str,
        item_id: &str,
        part_id: &str,
    ) -> Result<(), ConsumerError> {
        info!(
            run_id = run_id,
            item_id = item_id,
            part_id = part_id,
            "Resuming consumer from checkpoint"
        );

        // Load last checkpoint to verify state
        match self.coordinator.load_last_checkpoint().await? {
            Some(checkpoint) => {
                info!(
                    stage = %checkpoint.stage,
                    rows_done = checkpoint.rows_done,
                    cursor = ?checkpoint.src_offset,
                    "Loaded checkpoint, consumer will continue from last position"
                );

                // If we crashed during "write" stage, the producer will re-send
                // that batch based on its checkpoint recovery logic
                if checkpoint.stage == "write" {
                    warn!(
                        batch_id = %checkpoint.batch_id,
                        "Last batch was being written when crash occurred, \
                         it may be re-sent by producer"
                    );
                }
            }
            None => {
                info!("No checkpoint found, consumer starting fresh");
            }
        }

        self.mode = ConsumerMode::Running;
        Ok(())
    }

    async fn tick(&mut self) -> Result<ConsumerStatus, ConsumerError> {
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
                    info!("Consumer received stop signal, entering flush mode");
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
                            info!(
                                "Batch channel closed and all batches processed, consumer finished"
                            );
                            self.mode = ConsumerMode::Finished;
                            return Ok(ConsumerStatus::Finished);
                        }
                        // Channel still open, just idle
                        Ok(ConsumerStatus::Idle)
                    }
                    Err(e) => {
                        error!(error = %e, "Failed to process batch");
                        // On error, enter flushing mode to clean up
                        self.mode = ConsumerMode::Flushing;
                        Err(e)
                    }
                }
            }

            ConsumerMode::Flushing => {
                // Flush any pending writes before finishing
                info!("Flushing consumer writes");

                if let Err(e) = self.coordinator.try_process_one().await {
                    error!(error = %e, "Failed to flush on shutdown");
                }

                self.mode = ConsumerMode::Finished;
                Ok(ConsumerStatus::Finished)
            }
        }
    }

    async fn stop(&mut self) -> Result<(), ConsumerError> {
        info!(
            run_id = %self.ids.run_id(),
            item_id = %self.ids.item_id(),
            "Stopping LiveConsumer"
        );

        if self.config.disable_triggers {
            info!("Consumer configured to re-enable triggers on stop");
            // TODO: Re-enable triggers if applicable
        }

        self.mode = ConsumerMode::Finished;
        info!("LiveConsumer stopped successfully");
        Ok(())
    }

    fn rows_processed(&self) -> u64 {
        self.coordinator.rows_processed()
    }
}
