use crate::{
    error::ProducerError,
    producer::{DataProducer, pipeline_for_mapping},
    transform::pipeline::TransformPipeline,
};
use async_trait::async_trait;
use engine_core::{
    connectors::source::Source,
    context::item::ItemContext,
    state::{
        StateStore,
        models::{Checkpoint, WalEntry},
    },
};
use futures::{StreamExt, lock::Mutex, stream};
use model::{
    pagination::cursor::Cursor,
    records::{
        batch::{Batch, manifest_for},
        record::Record,
    },
};
use planner::query::offsets::OffsetStrategy;
use smql_syntax::ast::setting::Settings;
use std::sync::Arc;
use tokio::sync::{mpsc, watch::Sender};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

pub struct LiveProducer {
    // Identity
    pub run_id: String,
    pub item_id: String,
    pub part_id: String,

    // Strategy & state
    pub strategy: Arc<dyn OffsetStrategy>,
    pub cursor: Cursor,
    pub state_store: Arc<dyn StateStore>,

    // Transform
    pub pipeline: TransformPipeline,
    pub transform_concurrency: usize,

    // Control
    pub cancel: CancellationToken,
    pub heartbeat_every: usize,

    // IO
    pub src: Source,
    pub tx: Sender<bool>,
    pub batch_tx: mpsc::Sender<Batch>,
    pub batch_rows: usize,
}

impl LiveProducer {
    pub async fn new(
        ctx: &Arc<Mutex<ItemContext>>,
        shutdown_tx: Sender<bool>,
        batch_tx: mpsc::Sender<Batch>,
        settings: &Settings,
        cancel: CancellationToken,
    ) -> Self {
        let (run_id, item_id, part_id, strategy, cursor, state, source, mapping) = {
            let c = ctx.lock().await;
            (
                c.run_id.clone(),
                c.item_id.clone(),
                "part-0".to_string(), // For simplicity, using a fixed part ID for now
                c.offset_strategy.clone(),
                c.cursor.clone(),
                c.state.clone(),
                c.source.clone(),
                c.mapping.clone(),
            )
        };
        let pipeline = pipeline_for_mapping(&mapping);

        LiveProducer {
            run_id,
            item_id,
            part_id,
            strategy,
            cursor,
            state_store: state,
            pipeline,
            transform_concurrency: 8, // TODO: make configurable
            cancel,
            heartbeat_every: 10, // TODO: make configurable,
            src: source,
            tx: shutdown_tx,
            batch_tx,
            batch_rows: settings.batch_size,
        }
    }

    fn make_batch_id(&self, next: &Cursor) -> String {
        let mut h = blake3::Hasher::new();
        h.update(self.run_id.as_bytes());
        h.update(self.item_id.as_bytes());
        h.update(self.part_id.as_bytes());
        h.update(format!("{next:?}").as_bytes());
        h.finalize().to_hex().to_string()
    }
}

#[async_trait]
impl DataProducer for LiveProducer {
    async fn run(&mut self) -> Result<usize, ProducerError> {
        // Start from checkpoint if present
        let mut cur = self
            .state_store
            .load_checkpoint(&self.run_id, &self.item_id, &self.part_id)
            .await
            .ok()
            .flatten()
            .map(|cp| cp.src_offset)
            .unwrap_or(self.cursor.clone());

        let mut batches = 0usize;

        loop {
            if self.cancel.is_cancelled() {
                info!("Cancellation requested. Terminating producer.");
                break;
            }

            let res = self.src.fetch_data(self.batch_rows, cur.clone()).await?;
            if res.reached_end && res.row_count == 0 {
                info!("No more records to fetch. Terminating producer.");
                break;
            }

            if res.row_count == 0 {
                // Defensive: skip empty batch but advance cursor if provided
                if let Some(next) = res.next_cursor {
                    cur = next;
                    continue;
                }
                // No rows and no next cursor: treat as end
                break;
            }

            let next = match res.next_cursor {
                Some(n) => n,
                None if res.reached_end => cur.clone(), // last batch; 'next' equals current end
                None => {
                    warn!("Batch had rows but no next_cursor and not reached_end; skipping.");
                    break;
                }
            };

            let batch_id = self.make_batch_id(&next);
            info!(batch_no = batches + 1, batch_id = %batch_id, rows = res.row_count, "Fetched batch.");

            // WAL + checkpoint BEFORE enqueue (stage = read)
            self.state_store
                .append_wal(&WalEntry::BatchBegin {
                    run_id: self.run_id.clone(),
                    item_id: self.item_id.clone(),
                    part_id: self.part_id.clone(),
                    batch_id: batch_id.clone(),
                })
                .await
                .map_err(|e| ProducerError::StateStore(e.to_string()))?;

            self.state_store
                .save_checkpoint(&Checkpoint {
                    run_id: self.run_id.clone(),
                    item_id: self.item_id.clone(),
                    part_id: self.part_id.clone(),
                    stage: "read".to_string(),
                    src_offset: next.clone(),
                    batch_id: batch_id.clone(),
                    rows_done: res.row_count as u64,
                    updated_at: chrono::Utc::now(),
                })
                .await
                .map_err(|e| ProducerError::StateStore(e.to_string()))?;

            // Transform with bounded concurrency
            let records: Vec<Record> = stream::iter(res.rows.into_iter().map(|record| {
                let p = &self.pipeline;
                async move { p.apply(&Record::RowData(record)) }
            }))
            .buffer_unordered(self.transform_concurrency)
            .collect()
            .await;

            // Manifest (counts + checksum) for validation/ops
            let manifest = manifest_for(&records);

            // Enqueue batch (applies backpressure if consumer lags)
            let batch = Batch {
                id: batch_id,
                rows: records,
                next: next.clone(),
                manifest,
                ts: chrono::Utc::now(),
            };

            self.batch_tx
                .send(batch)
                .await
                .map_err(|e| ProducerError::ChannelSend(e.to_string()))?;

            // Advance and heartbeat
            cur = next;
            batches += 1;

            if batches % self.heartbeat_every == 0 {
                // lightweight liveness ping
                let _ = self
                    .state_store
                    .append_wal(&WalEntry::Heartbeat {
                        run_id: self.run_id.clone(),
                        item_id: self.item_id.clone(),
                        part_id: self.part_id.clone(),
                        at: chrono::Utc::now(),
                    })
                    .await;
            }
        }

        // Try to notify consumer; do not crash if the receiver is gone.
        let _ = self.tx.send(true);

        Ok(batches) // Return the number of batches processed
    }
}
