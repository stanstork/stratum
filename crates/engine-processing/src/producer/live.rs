use crate::{
    error::ProducerError,
    item::ItemId,
    producer::{DataProducer, ProducerStatus, pipeline_for_mapping},
    retry::classify_adapter_error,
    transform::pipeline::TransformPipeline,
};
use async_trait::async_trait;
use engine_core::{
    connectors::source::Source,
    context::item::ItemContext,
    retry::{RetryError, RetryPolicy},
    state::{
        StateStore,
        models::{Checkpoint, CheckpointSummary, WalEntry},
    },
};
use futures::{StreamExt, lock::Mutex, stream};
use model::{
    pagination::cursor::Cursor,
    records::{
        batch::{Batch, manifest_for},
        row::RowData,
    },
};
use planner::query::offsets::OffsetStrategy;
use smql_syntax::ast::setting::Settings;
use std::{num::NonZeroUsize, sync::Arc, time::Duration};
use tokio::sync::mpsc;
use tracing::{info, warn};

#[derive(Clone, Debug, PartialEq)]
pub enum ProducerMode {
    Idle,
    Snapshot,
    Cdc,
    Finished,
}

#[derive(Clone)]
pub struct LiveProducer {
    ids: ItemId,

    // Strategy & state
    pub offset_strategy: Arc<dyn OffsetStrategy>,
    pub cursor: Cursor,
    pub state_store: Arc<dyn StateStore>,

    // Transform
    pub pipeline: TransformPipeline,
    pub transform_concurrency: NonZeroUsize,

    // Communication
    pub batch_tx: mpsc::Sender<Batch>,
    pub batch_size: usize,

    // IO
    pub source: Source,
    pub retry: RetryPolicy,

    // Operational Mode
    pub mode: ProducerMode,
}

impl LiveProducer {
    pub async fn new(
        ctx: &Arc<Mutex<ItemContext>>,
        batch_tx: mpsc::Sender<Batch>,
        settings: &Settings,
    ) -> Self {
        let (run_id, item_id, part_id, offset_strategy, cursor, state_store, source, mapping) = {
            let c = ctx.lock().await;
            (
                c.run_id.clone(),
                c.item_id.clone(),
                "part-0".to_string(),
                c.offset_strategy.clone(),
                c.cursor.clone(),
                c.state.clone(),
                c.source.clone(),
                c.mapping.clone(),
            )
        };

        let pipeline = pipeline_for_mapping(&mapping);

        Self {
            ids: ItemId::new(run_id, item_id, part_id),
            offset_strategy,
            cursor,
            state_store,
            pipeline,
            transform_concurrency: NonZeroUsize::new(8).unwrap(),
            batch_tx,
            batch_size: settings.batch_size,
            source,
            retry: RetryPolicy::for_database(),
            mode: ProducerMode::Idle,
        }
    }

    fn batch_id(&self, next: &Cursor) -> String {
        let mut h = blake3::Hasher::new();
        h.update(self.ids.run_id().as_bytes());
        h.update(self.ids.item_id().as_bytes());
        h.update(self.ids.part_id().as_bytes());
        h.update(format!("{next:?}").as_bytes());
        h.finalize().to_hex().to_string()
    }

    /// Reconstruct the correct resume cursor based on the last checkpoint.
    ///
    /// Rules:
    /// - If stage="committed": resume from `src_offset` (fully committed)
    /// - If stage="read"/"write":
    ///     - If WAL contains BatchCommit for this batch -> resume from `pending_offset`
    ///     - Otherwise -> resume from `src_offset`
    /// - Otherwise: fallback to `src_offset`
    async fn cursor_from_checkpoint(&self, summary: &CheckpointSummary) -> Cursor {
        match summary.stage.as_str() {
            "committed" => summary.src_offset.clone(),
            "read" | "write" => {
                let wal_entries = match self.state_store.iter_wal(&self.ids.run_id()).await {
                    Ok(entries) => entries,
                    Err(err) => {
                        warn!(error = %err, "Failed to read WAL entries");
                        return summary.src_offset.clone();
                    }
                };
                if Self::wal_has_commit(&wal_entries, &self.ids, &summary.batch_id) {
                    summary
                        .pending_offset
                        .as_ref()
                        .cloned()
                        .unwrap_or(summary.src_offset.clone())
                } else {
                    summary.src_offset.clone()
                }
            }
            _ => summary.src_offset.clone(),
        }
    }

    /// Return true if a BatchCommit entry exists for this item/part/batch.
    ///
    /// Used to distinguish between “read but not written” and
    /// “written but crash before checkpoint”.
    fn wal_has_commit(entries: &[WalEntry], ids: &ItemId, batch_id: &str) -> bool {
        entries.iter().rev().any(|entry| {
            matches!(entry,
                WalEntry::BatchCommit { item_id, part_id, batch_id: b_id, .. }
                if *item_id == ids.item_id() && *part_id == ids.part_id() && b_id == batch_id
            )
        })
    }

    async fn transform(&self, rows: Vec<RowData>) -> Vec<RowData> {
        stream::iter(rows.into_iter().map(|row| {
            let transform_pipeline = &self.pipeline;
            async move { transform_pipeline.apply(&row) }
        }))
        .buffer_unordered(self.transform_concurrency.get())
        .collect()
        .await
    }

    async fn log_batch_start(
        &self,
        batch_id: &str,
        current: &Cursor,
        next: &Cursor,
    ) -> Result<(), ProducerError> {
        let rows_done = self
            .state_store
            .load_checkpoint(&self.ids.run_id(), &self.ids.item_id(), &self.ids.part_id())
            .await
            .map_err(|e| ProducerError::StateStore(e.to_string()))?
            .map(|cp| cp.rows_done)
            .unwrap_or(0);

        self.state_store
            .append_wal(&WalEntry::BatchBegin {
                run_id: self.ids.run_id(),
                item_id: self.ids.item_id(),
                part_id: self.ids.part_id(),
                batch_id: batch_id.to_string(),
            })
            .await
            .map_err(|e| ProducerError::StateStore(e.to_string()))?;

        self.state_store
            .save_checkpoint(&Checkpoint {
                run_id: self.ids.run_id(),
                item_id: self.ids.item_id(),
                part_id: self.ids.part_id(),
                stage: "read".to_string(),
                src_offset: current.clone(),
                pending_offset: Some(next.clone()),
                batch_id: batch_id.to_string(),
                rows_done,
                updated_at: chrono::Utc::now(),
            })
            .await
            .map_err(|e| ProducerError::StateStore(e.to_string()))
    }

    async fn send_batch(
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
            .send(batch)
            .await
            .map_err(|e| ProducerError::ChannelSend(e.to_string()))
    }
}

#[async_trait]
impl DataProducer for LiveProducer {
    async fn start_snapshot(&mut self) -> Result<(), ProducerError> {
        self.mode = ProducerMode::Snapshot;
        Ok(())
    }

    async fn start_cdc(&mut self) -> Result<(), ProducerError> {
        // CDC setup logic here
        self.mode = ProducerMode::Cdc;
        Ok(())
    }

    async fn resume(
        &mut self,
        run_id: &str,
        item_id: &str,
        part_id: &str,
    ) -> Result<(), ProducerError> {
        match self
            .state_store
            .last_checkpoint(run_id, item_id, part_id)
            .await
        {
            Ok(Some(summary)) => {
                self.cursor = self.cursor_from_checkpoint(&summary).await;
                info!("Resuming producer from cursor: {:?}", self.cursor);
            }
            Ok(None) => info!("No checkpoint found, starting from initial cursor."),
            Err(e) => warn!("Failed to load checkpoint: {}", e),
        }
        Ok(())
    }

    async fn tick(&mut self) -> Result<ProducerStatus, ProducerError> {
        match self.mode {
            ProducerMode::Idle => Ok(ProducerStatus::Idle),
            ProducerMode::Finished => Ok(ProducerStatus::Finished),

            ProducerMode::Snapshot => {
                // Fetch
                let source = self.source.clone();
                let cursor_template = self.cursor.clone();
                let batch_size = self.batch_size;

                let fetch_result = self
                    .retry
                    .run(
                        || {
                            let source = source.clone();
                            let cursor = cursor_template.clone();
                            async move { source.fetch_data(batch_size, cursor).await }
                        },
                        classify_adapter_error,
                    )
                    .await;

                let res = match fetch_result {
                    Ok(res) => res,
                    Err(RetryError::Fatal(e)) => return Err(ProducerError::Fetch(e)),
                    Err(RetryError::AttemptsExceeded(e)) => {
                        return Err(ProducerError::RetriesExhausted(e.to_string()));
                    }
                };

                // Handle End / Empty
                if res.reached_end && res.row_count == 0 {
                    self.mode = ProducerMode::Finished;
                    return Ok(ProducerStatus::Finished);
                }

                if res.row_count == 0 {
                    if let Some(next) = res.next_cursor
                        && next != Cursor::None
                    {
                        self.cursor = next;
                        return Ok(ProducerStatus::Working); // Continue working/advancing
                    }
                    self.mode = ProducerMode::Finished;
                    return Ok(ProducerStatus::Finished);
                }

                // Process Batch
                let batch_id = self.batch_id(&self.cursor);
                let next = res.next_cursor.clone().unwrap_or(Cursor::None);

                self.log_batch_start(&batch_id, &self.cursor, &next).await?;

                let transformed_rows = self.transform(res.rows).await;
                let batch = Batch {
                    id: batch_id,
                    rows: transformed_rows,
                    cursor: self.cursor.clone(),
                    next: next.clone(),
                    manifest: manifest_for(&vec![]),
                    ts: chrono::Utc::now(),
                };

                // Send to consumer
                self.batch_tx
                    .send(batch)
                    .await
                    .map_err(|e| ProducerError::ChannelSend(e.to_string()))?;

                // Update cursor
                self.cursor = next;

                // If this was the last page (no next cursor), we are effectively done with snapshot.
                if self.cursor == Cursor::None {
                    self.mode = ProducerMode::Finished;
                    return Ok(ProducerStatus::Finished);
                }

                Ok(ProducerStatus::Working)
            }

            ProducerMode::Cdc => {
                // Placeholder for CDC logic
                // Fetch events -> Normalize -> Send
                tokio::time::sleep(Duration::from_millis(100)).await;
                Ok(ProducerStatus::Working)
            }
        }
    }

    async fn stop(&mut self) -> Result<(), ProducerError> {
        self.mode = ProducerMode::Finished;
        Ok(())
    }
}
