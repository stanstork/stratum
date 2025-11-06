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
    pagination::{cursor::Cursor, page::FetchResult},
    records::{
        batch::{Batch, manifest_for},
        record::Record,
        row::RowData,
    },
};
use planner::query::offsets::OffsetStrategy;
use smql_syntax::ast::setting::Settings;
use std::sync::Arc;
use tokio::sync::{mpsc, watch::Sender};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

#[derive(Clone)]
struct Identity {
    run_id: String,
    item_id: String,
    part_id: String,
}

impl Identity {
    fn new(run_id: String, item_id: String, part_id: String) -> Self {
        Self {
            run_id,
            item_id,
            part_id,
        }
    }

    fn run_id(&self) -> &str {
        &self.run_id
    }

    fn item_id(&self) -> &str {
        &self.item_id
    }

    fn part_id(&self) -> &str {
        &self.part_id
    }
}

enum BatchFetchResult {
    Batch(FetchResult),
    AdvanceOnly(Cursor),
    Finished,
}

pub struct LiveProducer {
    identity: Identity,

    // Strategy & state
    pub offset: Arc<dyn OffsetStrategy>,
    pub cursor: Cursor,
    pub state: Arc<dyn StateStore>,

    // Transform
    pub transform_pipeline: TransformPipeline,
    pub transform_concurrency: usize,

    // Control
    pub cancel: CancellationToken,
    pub heartbeat_interval: usize,

    // IO
    pub source: Source,
    pub shutdown_tx: Sender<bool>,
    pub batch_tx: mpsc::Sender<Batch>,
    pub batch_size: usize,
}

impl LiveProducer {
    pub async fn new(
        ctx: &Arc<Mutex<ItemContext>>,
        shutdown_tx: Sender<bool>,
        batch_tx: mpsc::Sender<Batch>,
        settings: &Settings,
        cancel: CancellationToken,
    ) -> Self {
        let (run_id, item_id, part_id, offset, cursor, state, source, mapping) = {
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
            identity: Identity::new(run_id, item_id, part_id),
            offset,
            cursor,
            state,
            transform_pipeline: pipeline,
            transform_concurrency: 8, // TODO: make configurable
            cancel,
            heartbeat_interval: 10, // TODO: make configurable,
            source,
            shutdown_tx,
            batch_tx,
            batch_size: settings.batch_size,
        }
    }

    fn make_batch_id(&self, next: &Cursor) -> String {
        let mut h = blake3::Hasher::new();
        h.update(self.identity.run_id.as_bytes());
        h.update(self.identity.item_id.as_bytes());
        h.update(self.identity.part_id.as_bytes());
        h.update(format!("{next:?}").as_bytes());
        h.finalize().to_hex().to_string()
    }

    async fn starting_cursor(&self) -> Cursor {
        self.state
            .load_checkpoint(
                self.identity.run_id(),
                self.identity.item_id(),
                self.identity.part_id(),
            )
            .await
            .ok()
            .flatten()
            .map(|cp| cp.src_offset)
            .unwrap_or(self.cursor.clone())
    }

    async fn fetch_next_batch(&self, cursor: &Cursor) -> Result<BatchFetchResult, ProducerError> {
        let res = self
            .source
            .fetch_data(self.batch_size, cursor.clone())
            .await?;

        if res.reached_end && res.row_count == 0 {
            info!("No more records to fetch. Terminating producer.");
            return Ok(BatchFetchResult::Finished);
        }

        if res.row_count == 0 {
            if let Some(next) = res.next_cursor {
                return Ok(BatchFetchResult::AdvanceOnly(next));
            }
            return Ok(BatchFetchResult::Finished);
        }

        Ok(BatchFetchResult::Batch(res))
    }

    fn next_cursor(&self, res: &FetchResult, current: &Cursor) -> Option<Cursor> {
        match res.next_cursor.clone() {
            Some(next) => Some(next),
            None if res.reached_end => Some(current.clone()),
            None => {
                warn!("Batch had rows but no next_cursor and not reached_end; skipping.");
                None
            }
        }
    }

    async fn record_batch_start(
        &self,
        batch_id: &str,
        next: &Cursor,
        row_count: usize,
    ) -> Result<(), ProducerError> {
        self.state
            .append_wal(&WalEntry::BatchBegin {
                run_id: self.identity.run_id.clone(),
                item_id: self.identity.item_id.clone(),
                part_id: self.identity.part_id.clone(),
                batch_id: batch_id.to_string(),
            })
            .await
            .map_err(|e| ProducerError::StateStore(e.to_string()))?;

        self.state
            .save_checkpoint(&Checkpoint {
                run_id: self.identity.run_id.clone(),
                item_id: self.identity.item_id.clone(),
                part_id: self.identity.part_id.clone(),
                stage: "read".to_string(),
                src_offset: next.clone(),
                batch_id: batch_id.to_string(),
                rows_done: row_count as u64,
                updated_at: chrono::Utc::now(),
            })
            .await
            .map_err(|e| ProducerError::StateStore(e.to_string()))
    }

    async fn transform(&self, rows: Vec<RowData>) -> Vec<Record> {
        stream::iter(rows.into_iter().map(|row| {
            let transform_pipeline = &self.transform_pipeline;
            async move {
                let record = Record::RowData(row);
                transform_pipeline.apply(&record)
            }
        }))
        .buffer_unordered(self.transform_concurrency)
        .collect()
        .await
    }

    async fn send_batch(
        &self,
        batch_id: String,
        records: Vec<Record>,
        next: Cursor,
    ) -> Result<(), ProducerError> {
        let manifest = manifest_for(&records);
        let batch = Batch {
            id: batch_id,
            rows: records,
            next,
            manifest,
            ts: chrono::Utc::now(),
        };

        self.batch_tx
            .send(batch)
            .await
            .map_err(|e| ProducerError::ChannelSend(e.to_string()))
    }

    async fn heartbeat(&self, batches: usize) {
        if self.heartbeat_interval == 0 || batches % self.heartbeat_interval != 0 {
            return;
        }

        let _ = self
            .state
            .append_wal(&WalEntry::Heartbeat {
                run_id: self.identity.run_id.clone(),
                item_id: self.identity.item_id.clone(),
                part_id: self.identity.part_id.clone(),
                at: chrono::Utc::now(),
            })
            .await;
    }

    fn notify_shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }
}

#[async_trait]
impl DataProducer for LiveProducer {
    async fn run(&mut self) -> Result<usize, ProducerError> {
        let mut cur = self.starting_cursor().await;
        let mut batches = 0usize;

        loop {
            if self.cancel.is_cancelled() {
                info!("Cancellation requested. Terminating producer.");
                break;
            }

            match self.fetch_next_batch(&cur).await? {
                BatchFetchResult::Finished => break,
                BatchFetchResult::AdvanceOnly(next) => {
                    cur = next;
                    continue;
                }
                BatchFetchResult::Batch(res) => {
                    let next = match self.next_cursor(&res, &cur) {
                        Some(cursor) => cursor,
                        None => break,
                    };

                    let batch_id = self.make_batch_id(&next);
                    info!(
                        batch_no = batches + 1,
                        batch_id = %batch_id,
                        rows = res.row_count,
                        "Fetched batch."
                    );

                    self.record_batch_start(&batch_id, &next, res.row_count)
                        .await?;

                    let records = self.transform(res.rows).await;
                    self.send_batch(batch_id, records, next.clone()).await?;

                    cur = next;
                    batches += 1;
                    self.heartbeat(batches).await;
                }
            }
        }

        self.notify_shutdown();

        Ok(batches) // Return the number of batches processed
    }
}
