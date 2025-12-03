use crate::{
    consumer::components::writer::BatchWriter, error::ConsumerError, state_manager::StateManager,
};
use engine_core::{metrics::Metrics, state::models::Checkpoint};
use model::records::batch::Batch;
use tokio::sync::mpsc;
use tracing::info;

/// Coordinates batch receiving, writing, and checkpointing.
pub struct BatchCoordinator {
    writer: BatchWriter,
    state_manager: StateManager,
    metrics: Metrics,
    batch_rx: mpsc::Receiver<Batch>,
}

impl BatchCoordinator {
    pub fn new(
        writer: BatchWriter,
        state_manager: StateManager,
        metrics: Metrics,
        batch_rx: mpsc::Receiver<Batch>,
    ) -> Self {
        Self {
            writer,
            state_manager,
            metrics,
            batch_rx,
        }
    }

    /// Try to receive and process one batch.
    pub async fn try_process_one(&mut self) -> Result<bool, ConsumerError> {
        match self.batch_rx.try_recv() {
            Ok(batch) => {
                self.process_batch(batch).await?;
                Ok(true)
            }
            Err(mpsc::error::TryRecvError::Empty) => Ok(false),
            Err(mpsc::error::TryRecvError::Disconnected) => {
                info!("Batch channel disconnected");
                Ok(false)
            }
        }
    }

    pub fn is_channel_closed(&self) -> bool {
        self.batch_rx.is_closed()
    }

    /// Process a single batch: write + checkpoint + metrics.
    pub async fn process_batch(&self, batch: Batch) -> Result<(), ConsumerError> {
        let batch_id = batch.id.clone();
        let row_count = batch.rows.len();

        info!(
            batch_id = %batch_id,
            rows = row_count,
            cursor = ?batch.cursor,
            next = ?batch.next,
            "Processing batch"
        );

        // Get current progress
        let current_rows = self.get_progress().await?;

        // Mark as being written
        self.state_manager
            .save_checkpoint(
                "write",
                &batch.cursor,
                Some(&batch.next),
                &batch.id,
                current_rows,
            )
            .await
            .map_err(|e| ConsumerError::Checkpoint {
                batch_id: batch.id.clone(),
                source: Box::new(e),
            })?;

        // Write to destination with retry
        let write_result = self.writer.write_batch(&batch).await?;

        let new_rows = current_rows + row_count as u64;

        // Mark as committed
        self.state_manager
            .commit_batch(&batch.id)
            .await
            .map_err(|e| ConsumerError::Checkpoint {
                batch_id: batch.id.clone(),
                source: Box::new(e),
            })?;

        self.state_manager
            .save_checkpoint("committed", &batch.next, None, &batch.id, new_rows)
            .await
            .map_err(|e| ConsumerError::Checkpoint {
                batch_id: batch.id.clone(),
                source: Box::new(e),
            })?;

        self.metrics.increment_records(row_count as u64);
        self.metrics.increment_batches(1);

        info!(
            batch_id = %batch_id,
            rows = row_count,
            total_rows = new_rows,
            strategy = ?write_result.strategy,
            "Batch processed successfully"
        );

        Ok(())
    }

    pub async fn load_last_checkpoint(&self) -> Result<Option<Checkpoint>, ConsumerError> {
        self.state_manager
            .load_checkpoint()
            .await
            .map_err(|e| ConsumerError::StateLoad {
                source: Box::new(e),
            })
    }

    pub fn rows_processed(&self) -> u64 {
        self.metrics.snapshot().records_processed
    }

    async fn get_progress(&self) -> Result<u64, ConsumerError> {
        Ok(self
            .state_manager
            .load_checkpoint()
            .await
            .map_err(|e| ConsumerError::StateLoad {
                source: Box::new(e),
            })?
            .map(|cp| cp.rows_done)
            .unwrap_or(0))
    }
}
