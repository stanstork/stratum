use engine_core::metrics::Metrics;
use model::records::batch::Batch;
use tokio::sync::mpsc;
use tracing::{error, info};

use crate::{
    consumer::{components::writer::BatchWriter, config::ConsumerConfig},
    error::ConsumerError,
    state_manager::StateManager,
};

/// Orchestrates the batch processing workflow.
pub struct BatchProcessor {
    writer: BatchWriter,
    state_manager: StateManager,
    metrics: Metrics,
    config: ConsumerConfig,
    batch_rx: mpsc::Receiver<Batch>,
}

impl BatchProcessor {
    pub fn new(
        writer: BatchWriter,
        state_manager: StateManager,
        metrics: Metrics,
        config: ConsumerConfig,
        batch_rx: mpsc::Receiver<Batch>,
    ) -> Self {
        Self {
            writer,
            state_manager,
            metrics,
            config,
            batch_rx,
        }
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

        // 1. Get current progress
        let current_rows = self.state_manager.get_progress().await?;

        // 2. Mark as being written
        self.state_manager
            .mark_writing(&batch, current_rows)
            .await?;

        // 3. Write to destination with retry
        let write_result = match self.writer.write_batch(&batch).await {
            Ok(result) => result,
            Err(e) => {
                error!(
                    batch_id = %batch_id,
                    error = %e,
                    "Failed to write batch after retries"
                );
                self.metrics.increment_failures(1);
                return Err(e);
            }
        };

        // 4. Update progress
        let new_rows = current_rows + row_count as u64;

        // 5. Mark as committed
        self.state_manager.mark_committed(&batch, new_rows).await?;

        // 6. Update metrics
        self.metrics.increment_records(row_count as u64);
        self.metrics.record_batch_duration(write_result.duration);

        info!(
            batch_id = %batch_id,
            rows = row_count,
            total_rows = new_rows,
            duration_ms = write_result.duration.as_millis(),
            "Batch processed successfully"
        );

        Ok(())
    }
}
