use crate::{error::ProducerError, state_manager::StateManager};
use model::{
    pagination::cursor::Cursor,
    records::{
        batch::{Batch, manifest_for},
        row::RowData,
    },
};
use tokio::sync::mpsc;

/// Coordinates batch creation and delivery to consumers.
pub struct BatchCoordinator {
    batch_tx: Option<mpsc::Sender<Batch>>,
    state_manager: StateManager,
    rows_produced: u64,
}

impl BatchCoordinator {
    pub fn new(batch_tx: mpsc::Sender<Batch>, state_manager: StateManager) -> Self {
        Self {
            batch_tx: Some(batch_tx),
            state_manager,
            rows_produced: 0,
        }
    }

    /// Send a batch to the consumer channel.
    pub async fn send_batch(
        &self,
        batch_id: String,
        cursor: Cursor,
        rows: Vec<RowData>,
        next: Cursor,
    ) -> Result<(), ProducerError> {
        let manifest = manifest_for(&rows);
        let batch = Batch {
            id: batch_id,
            rows,
            cursor,
            next,
            manifest,
            ts: chrono::Utc::now(),
        };

        self.batch_tx
            .as_ref()
            .ok_or_else(|| ProducerError::ChannelSend("Channel already closed".to_string()))?
            .send(batch)
            .await
            .map_err(|e| ProducerError::ChannelSend(e.to_string()))
    }

    /// Close the batch channel to signal consumers that no more batches will be sent.
    /// This should be called when the producer has finished sending all data.
    pub fn close_channel(&mut self) {
        self.batch_tx = None;
    }

    /// Complete batch lifecycle: log start, send, and optionally commit.
    pub async fn process_batch(
        &mut self,
        batch_id: String,
        current_cursor: Cursor,
        rows: Vec<RowData>,
        next_cursor: Cursor,
    ) -> Result<(), ProducerError> {
        let rows_count = rows.len();

        // Log batch start for crash recovery
        self.state_manager
            .begin_batch(&batch_id, &current_cursor, &next_cursor)
            .await?;

        // Send to consumer
        self.send_batch(batch_id.clone(), current_cursor, rows, next_cursor)
            .await?;

        self.state_manager.commit_batch(&batch_id).await?;
        self.rows_produced += rows_count as u64;

        Ok(())
    }

    pub fn state_manager(&self) -> &StateManager {
        &self.state_manager
    }

    pub fn rows_produced(&self) -> u64 {
        self.rows_produced
    }
}
