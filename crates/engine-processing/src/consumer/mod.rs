use crate::channel::BatchEnvelope;
use crate::context::PipelineContext;
use crate::io::destination::Destination;
use crate::{
    consumer::components::{coordinator::BatchCoordinator, writer::BatchWriter},
    error::ConsumerError,
    item::ItemId,
    state_manager::StateManager,
};
use connectors::sql::metadata::table::TableMetadata;
use engine_infra::{metrics::Metrics, retry::RetryPolicy};
use engine_state::models::CheckpointStage;
use tokio::sync::mpsc;
use tracing::{debug, warn};

pub mod components;
pub mod config;

pub struct Consumer {
    coordinator: BatchCoordinator,
    ids: ItemId,
}

impl Consumer {
    pub async fn new(
        ctx: &PipelineContext,
        destination: Destination,
        batch_rx: mpsc::Receiver<BatchEnvelope>,
        dest_metadata: Vec<TableMetadata>,
        part_id: &str,
        metrics: Metrics,
    ) -> Self {
        let run_id = ctx.run_id.clone();
        let item_id = ctx.item_id.clone();
        let pipeline = ctx.pipeline.clone();
        let state_store = ctx.state.clone();

        let ids = ItemId::new(run_id, item_id, part_id.to_string());

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

        Self { coordinator, ids }
    }

    pub async fn start(&mut self) -> Result<(), ConsumerError> {
        debug!(
            run_id = %self.ids.run_id(),
            item_id = %self.ids.item_id(),
            "starting consumer"
        );

        // Sink one-time setup before any batch is written.
        self.coordinator.prepare().await?;

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

        Ok(())
    }

    /// Await the next batch from the producer (cancel-safe). `None` means the
    /// producer closed the channel and it is drained.
    pub async fn recv_batch(&mut self) -> Option<BatchEnvelope> {
        self.coordinator.recv().await
    }

    /// Write one received batch (COPY + checkpoint + metrics).
    pub async fn write(&mut self, envelope: &BatchEnvelope) -> Result<(), ConsumerError> {
        self.coordinator.process_batch(&envelope.batch).await
    }

    pub async fn stop(&mut self) -> Result<(), ConsumerError> {
        debug!(
            run_id = %self.ids.run_id(),
            item_id = %self.ids.item_id(),
            "stopping consumer"
        );

        debug!("consumer stopped");
        Ok(())
    }

    pub fn rows_written(&self) -> u64 {
        self.coordinator.rows_processed()
    }
}
