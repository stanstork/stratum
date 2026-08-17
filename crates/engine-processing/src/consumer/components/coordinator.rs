use crate::{
    channel::BatchEnvelope, consumer::components::writer::BatchWriter, error::ConsumerError,
    profile, state_manager::StateManager,
};
use engine_infra::metrics::Metrics;
use engine_state::models::Checkpoint;
use engine_state::models::CheckpointStage;
use model::records::batch::Batch;
use tokio::sync::mpsc;
use tracing::debug;

/// Coordinates batch receiving, writing, and checkpointing.
pub struct BatchCoordinator {
    writer: BatchWriter,
    state_manager: StateManager,
    metrics: Metrics,
    batch_rx: mpsc::Receiver<BatchEnvelope>,
}

impl BatchCoordinator {
    pub fn new(
        writer: BatchWriter,
        state_manager: StateManager,
        metrics: Metrics,
        batch_rx: mpsc::Receiver<BatchEnvelope>,
    ) -> Self {
        Self {
            writer,
            state_manager,
            metrics,
            batch_rx,
        }
    }

    pub async fn prepare(&self) -> Result<(), ConsumerError> {
        self.writer.prepare().await
    }

    pub async fn finalize(&self) -> Result<(), ConsumerError> {
        self.writer.finalize().await
    }

    /// Await the next batch envelope.
    pub async fn recv(&mut self) -> Option<BatchEnvelope> {
        self.batch_rx.recv().await
    }

    /// Process a single batch: write + checkpoint + metrics.
    pub async fn process_batch(&self, batch: &Batch) -> Result<(), ConsumerError> {
        use std::time::Instant;

        let t_total = Instant::now();
        let batch_id = batch.id.clone();
        let row_count = batch.rows.len();
        let byte_count = batch.size_bytes();

        debug!(
            batch_id = %batch_id,
            rows = row_count,
            bytes = byte_count,
            cursor = ?batch.cursor,
            next = ?batch.next,
            "processing batch"
        );

        // Get current progress
        let current_rows = self.get_progress().await?;

        // Mark as being written
        self.state_manager
            .save_checkpoint(
                &CheckpointStage::Write,
                &batch.cursor,
                Some(&batch.next),
                &batch.id,
                current_rows,
            )
            .await
            .map_err(|e| ConsumerError::Checkpoint {
                batch_id: batch.id.clone(),
                source: e,
            })?;

        // Write to destination with retry
        let t_write = Instant::now();
        let write_result = self.writer.write_batch(batch).await?;
        let write_dur = t_write.elapsed();

        profile::record(&profile::WRITE, write_dur);

        let new_rows = current_rows + row_count as u64;

        // Mark as committed
        self.state_manager
            .commit_batch(&batch.id)
            .await
            .map_err(|e| ConsumerError::Checkpoint {
                batch_id: batch.id.clone(),
                source: e,
            })?;

        self.state_manager
            .save_checkpoint(
                &CheckpointStage::Committed,
                &batch.next,
                None,
                &batch.id,
                new_rows,
            )
            .await
            .map_err(|e| ConsumerError::Checkpoint {
                batch_id: batch.id.clone(),
                source: e,
            })?;

        self.metrics.increment_records(row_count as u64);
        self.metrics.increment_bytes(byte_count as u64);
        self.metrics.increment_batches(1);

        debug!(
            batch_id = %batch_id,
            rows = row_count,
            bytes = byte_count,
            total_rows = new_rows,
            strategy = ?write_result.strategy,
            "batch processed"
        );

        profile::record(
            &profile::CHECKPOINT,
            t_total.elapsed().saturating_sub(write_dur),
        );

        Ok(())
    }

    pub async fn load_last_checkpoint(&self) -> Result<Option<Checkpoint>, ConsumerError> {
        Ok(self.state_manager.load_checkpoint().await?)
    }

    pub fn rows_processed(&self) -> u64 {
        self.metrics.snapshot().records_processed
    }

    async fn get_progress(&self) -> Result<u64, ConsumerError> {
        Ok(self
            .state_manager
            .load_checkpoint()
            .await?
            .map(|cp| cp.rows_done)
            .unwrap_or(0))
    }
}
