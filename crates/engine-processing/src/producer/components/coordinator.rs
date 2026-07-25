use crate::{
    channel::{BatchEnvelope, ByteBudget},
    error::ProducerError,
    producer::components::{
        integrity::{IntegrityState, LaneHashSink},
        transformer::TransformResult,
    },
    state_manager::StateManager,
};
use engine_state::MerkleStore;
use model::{
    integrity::config::IntegrityConfig,
    pagination::cursor::Cursor,
    records::{Record, batch::Batch},
};
use std::sync::Arc;
use tokio::sync::mpsc;

/// Coordinates batch creation and delivery to consumers.
pub struct BatchCoordinator {
    batch_tx: Option<mpsc::Sender<BatchEnvelope>>,
    byte_budget: ByteBudget,
    state_manager: StateManager,
    batches_processed: u64,
    rows_produced: u64,
    rows_skipped: u64,
    rows_failed: u64,
    integrity: Option<IntegrityState>,
}

impl BatchCoordinator {
    pub fn new(
        batch_tx: mpsc::Sender<BatchEnvelope>,
        byte_budget: ByteBudget,
        state_manager: StateManager,
    ) -> Self {
        Self {
            batch_tx: Some(batch_tx),
            byte_budget,
            state_manager,
            batches_processed: 0,
            rows_produced: 0,
            rows_skipped: 0,
            rows_failed: 0,
            integrity: None,
        }
    }

    /// Enable integrity hashing for this coordinator.
    pub fn enable_integrity(
        mut self,
        config: IntegrityConfig,
        merkle_store: Arc<dyn MerkleStore>,
        lane_sink: Option<LaneHashSink>,
    ) -> Self {
        let mut state = IntegrityState::new(config, merkle_store);
        if let Some(sink) = lane_sink {
            state = state.with_lane_sink(sink);
        }
        self.integrity = Some(state);
        self
    }

    /// Close the batch channel to signal consumers that no more batches will be sent.
    pub fn close_channel(&mut self) {
        self.batch_tx = None;
    }

    /// Hash (for integrity), send the batch to the consumer, and record stats.
    pub async fn process_batch(
        &mut self,
        batch_id: String,
        current_cursor: Cursor,
        transform_result: TransformResult,
        next_cursor: Cursor,
    ) -> Result<(), ProducerError> {
        let rows = transform_result.rows;
        let rows_count = rows.len();

        if let Some(ref mut state) = self.integrity {
            state.hash_batch(&rows);
        }

        // Send to consumer (which checkpoints after a successful write).
        self.send_batch(batch_id, current_cursor, rows, next_cursor)
            .await?;

        // Only record stats after successful processing.
        self.batches_processed += 1;
        self.rows_produced += rows_count as u64;
        self.rows_skipped += transform_result.rows_skipped;
        self.rows_failed += transform_result.rows_failed;

        Ok(())
    }

    /// Build per-table Merkle receipts and persist to sled. Call once after the last batch.
    pub async fn finalize_integrity(&mut self, pipeline_name: &str) -> Result<(), ProducerError> {
        let run_id = self.state_manager.ids().run_id();
        let rows_skipped = self.rows_skipped;
        let Some(ref mut state) = self.integrity else {
            return Ok(());
        };
        state
            .save_receipts(pipeline_name, &run_id, rows_skipped)
            .await
    }

    pub fn state_manager(&self) -> &StateManager {
        &self.state_manager
    }

    pub fn batches_processed(&self) -> u64 {
        self.batches_processed
    }

    pub fn rows_produced(&self) -> u64 {
        self.rows_produced
    }

    pub fn rows_skipped(&self) -> u64 {
        self.rows_skipped
    }

    pub fn rows_failed(&self) -> u64 {
        self.rows_failed
    }

    async fn send_batch(
        &self,
        batch_id: String,
        cursor: Cursor,
        rows: Vec<Record>,
        next: Cursor,
    ) -> Result<(), ProducerError> {
        let batch = Batch {
            id: batch_id,
            rows,
            cursor,
            next,
            ts: chrono::Utc::now(),
        };

        // Reserve the batch's byte-budget slice *before* handing it off, so a
        // wide-row batch blocks here until the in-flight total has room.
        let permit = self.byte_budget.reserve(batch.size_bytes()).await;

        self.batch_tx
            .as_ref()
            .ok_or(ProducerError::ChannelClosed)?
            .send(BatchEnvelope::new(batch, permit))
            .await
            .map_err(|_| ProducerError::ChannelClosed)
    }
}
