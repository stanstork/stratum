use crate::{
    error::ProducerError,
    item::ItemId,
    producer::{DataProducer, pipeline_for_mapping},
    transform::pipeline::TransformPipeline,
};
use async_trait::async_trait;
use engine_core::{
    connectors::source::Source,
    context::item::ItemContext,
    state::{
        StateStore,
        models::{Checkpoint, CheckpointSummary, WalEntry},
    },
};
use futures::{StreamExt, lock::Mutex, stream};
use model::{
    pagination::{cursor::Cursor, page::FetchResult},
    records::{
        batch::{Batch, manifest_for},
        record::Record,
        row::RowData,
    },
};
use planner::query::offsets::OffsetStrategy;
use smql_syntax::ast::setting::Settings;
use std::{num::NonZeroUsize, sync::Arc, time::Duration};
use tokio::sync::{mpsc, watch::Sender};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

enum FetchOutcome {
    Page(FetchResult),
    Advance(Cursor),
    End,
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

    // Control
    pub shutdown_tx: Sender<bool>,
    pub cancel_token: CancellationToken,
    pub heartbeat_interval: Duration,

    // IO
    pub source: Source,
    pub batch_tx: mpsc::Sender<Batch>,
    pub batch_size: usize,
}

impl LiveProducer {
    pub async fn new(
        ctx: &Arc<Mutex<ItemContext>>,
        shutdown_tx: Sender<bool>,
        batch_tx: mpsc::Sender<Batch>,
        settings: &Settings,
        cancel_token: CancellationToken,
    ) -> Self {
        let (run_id, item_id, part_id, offset_strategy, cursor, state_store, source, mapping) = {
            let c = ctx.lock().await;
            (
                c.run_id.clone(),
                c.item_id.clone(),
                "part-0".to_string(), // fixed part for now
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
            transform_concurrency: NonZeroUsize::new(8).unwrap(), // TODO: configurable
            cancel_token,
            heartbeat_interval: Duration::from_secs(30), // send every 30s by default. TODO: configurable
            source,
            shutdown_tx,
            batch_tx,
            batch_size: settings.batch_size,
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

    async fn start_cursor(&self) -> Cursor {
        match self
            .state_store
            .last_checkpoint(&self.ids.run_id(), &self.ids.item_id(), &self.ids.part_id())
            .await
        {
            Ok(Some(summary)) => self.cursor_from_checkpoint(&summary).await,
            Ok(None) => self.cursor.clone(),
            Err(err) => {
                warn!(
                    error = %err,
                    "Failed to load checkpoint; falling back to initial cursor"
                );
                self.cursor.clone()
            }
        }
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
                        warn!(
                            error = %err,
                            "Failed to read WAL entries; defaulting to prior cursor"
                        );
                        return summary.src_offset.clone();
                    }
                };

                if Self::wal_has_commit(&wal_entries, &self.ids, &summary.batch_id) {
                    summary
                        .pending_offset
                        .as_ref()
                        .cloned()
                        .unwrap_or_else(|| summary.src_offset.clone())
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
        let target_item = ids.item_id();
        let target_part = ids.part_id();
        entries.iter().rev().any(|entry| match entry {
            WalEntry::BatchCommit {
                item_id,
                part_id,
                batch_id: wal_batch_id,
                ..
            } => item_id == &target_item && part_id == &target_part && wal_batch_id == batch_id,
            _ => false,
        })
    }

    async fn next_page(&self, cursor: &Cursor) -> Result<FetchOutcome, ProducerError> {
        let res = self
            .source
            .fetch_data(self.batch_size, cursor.clone())
            .await?;

        if res.reached_end && res.row_count == 0 {
            info!("No more records to fetch. Terminating producer.");
            return Ok(FetchOutcome::End);
        }

        if res.row_count == 0 {
            if let Some(next) = res.next_cursor
                && next != Cursor::None
            {
                return Ok(FetchOutcome::Advance(next));
            }
            return Ok(FetchOutcome::End);
        }

        Ok(FetchOutcome::Page(res))
    }

    async fn log_batch_start(
        &self,
        batch_id: &str,
        current: &Cursor,
        next: &Cursor,
        row_count: usize,
    ) -> Result<(), ProducerError> {
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
                rows_done: row_count as u64,
                updated_at: chrono::Utc::now(),
            })
            .await
            .map_err(|e| ProducerError::StateStore(e.to_string()))
    }

    async fn transform(&self, rows: Vec<RowData>) -> Vec<Record> {
        stream::iter(rows.into_iter().map(|row| {
            let transform_pipeline = &self.pipeline;
            async move {
                let record = Record::RowData(row);
                transform_pipeline.apply(&record)
            }
        }))
        .buffer_unordered(self.transform_concurrency.get())
        .collect()
        .await
    }

    async fn send_batch(
        &self,
        batch_id: String,
        cursor: Cursor,
        records: Vec<Record>,
        next: Cursor,
    ) -> Result<(), ProducerError> {
        let manifest = manifest_for(&records);
        let mut rows = Vec::with_capacity(records.len());
        for record in records {
            rows.push(Record::to_row_data(record));
        }

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

    async fn heartbeat(self: Arc<Self>) {
        let mut interval = tokio::time::interval(self.heartbeat_interval);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if self.cancel_token.is_cancelled() {
                        break;
                    }
                    let _ = self.state_store
                        .append_wal(&WalEntry::Heartbeat {
                            run_id: self.ids.run_id(),
                            item_id: self.ids.item_id(),
                            part_id: self.ids.part_id(),
                            at: chrono::Utc::now(),
                        })
                        .await;
                }
                _ = self.cancel_token.cancelled() => break,
            }
        }
    }

    fn signal_shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }
}

#[async_trait]
impl DataProducer for LiveProducer {
    async fn run(&mut self) -> Result<usize, ProducerError> {
        let mut cur = self.start_cursor().await;
        let mut batches = 0usize;

        // Start heartbeat loop
        let heartbeat_self = Arc::new(self.clone());
        let hb_handle = {
            let cloned = heartbeat_self.clone();
            tokio::spawn(async move { cloned.heartbeat().await });
        };

        loop {
            if self.cancel_token.is_cancelled() {
                info!("Cancellation requested. Terminating producer.");
                break;
            }

            match self.next_page(&cur).await? {
                FetchOutcome::End => break,
                FetchOutcome::Advance(next) => {
                    cur = next;
                    continue;
                }
                FetchOutcome::Page(res) => {
                    let batch_id = self.batch_id(&cur);
                    info!(
                        batch_no = batches + 1,
                        batch_id = %batch_id,
                        rows = res.row_count,
                        "Fetched batch."
                    );

                    let next = res.next_cursor.clone().unwrap_or(Cursor::None);
                    let current_cursor = cur.clone();
                    self.log_batch_start(&batch_id, &current_cursor, &next, res.row_count)
                        .await?;

                    let transformed = self.transform(res.rows).await;
                    self.send_batch(batch_id, current_cursor.clone(), transformed, next.clone())
                        .await?;

                    if next != Cursor::None {
                        cur = next;
                        batches += 1;
                    } else {
                        warn!("No next cursor available; terminating producer.");
                        break;
                    }
                }
            }
        }

        self.signal_shutdown();
        let _ = hb_handle; // Wait for heartbeat loop to finish

        Ok(batches) // Return the number of batches processed
    }
}
