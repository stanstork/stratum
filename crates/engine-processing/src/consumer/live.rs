use crate::{consumer::DataConsumer, error::ConsumerError, item::ItemId};
use async_trait::async_trait;
use connectors::sql::base::metadata::table::TableMetadata;
use engine_config::report::metrics::{MetricsReport, send_report};
use engine_core::{
    connectors::{
        destination::{DataDestination, Destination},
        sink::Sink,
    },
    context::item::ItemContext,
    metrics::Metrics,
    state::{
        StateStore,
        models::{Checkpoint, WalEntry},
    },
};
use futures::lock::Mutex;
use model::{pagination::cursor::Cursor, records::batch::Batch};
use std::{sync::Arc, time::Instant};
use tokio::sync::{mpsc, mpsc::error::TryRecvError, watch::Receiver};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

pub struct LiveConsumer {
    ids: ItemId,

    // Shared context
    pub state_store: Arc<dyn StateStore>,
    pub destination: Destination,
    pub meta: Vec<TableMetadata>,

    // IO
    pub batch_rx: mpsc::Receiver<Batch>,

    // Control
    pub shutdown_rx: Receiver<bool>,
    pub cancel: CancellationToken,
}

#[async_trait]
impl DataConsumer for LiveConsumer {
    /// Main entry point for the consumer.
    /// Runs a loop to receive and process batches until the channel closes or cancellation is requested.
    async fn run(&mut self) -> Result<(), ConsumerError> {
        let start_time = Instant::now();
        let sink = self.destination.sink();
        let metrics = Metrics::new();

        info!("Consumer starting. Listening for batches...");

        loop {
            tokio::select! {
                biased;

                _ = self.cancel.cancelled() => {
                    info!("Cancellation requested. Exiting consumer loop.");
                    break;
                }

                batch = self.batch_rx.recv() => {
                    match batch {
                        Some(batch) => {
                            // Process the batch, propagating any errors to stop the consumer
                            self.process_batch(batch, sink.as_ref(), &metrics).await?;
                        }
                        None => {
                            info!("Batch channel closed. Exiting consumer loop.");
                            break;
                        }
                    }
                }

                changed = self.shutdown_rx.changed() => {
                    match changed {
                        Ok(_) if *self.shutdown_rx.borrow() => {
                            info!("Shutdown signal received. Draining pending batches before exit.");
                            self
                                .drain_pending_batches(sink.as_ref(), &metrics)
                                .await?;
                            break;
                        }
                        Ok(_) => {}
                        Err(_) => {
                            info!("Shutdown channel closed. Exiting consumer loop.");
                            break;
                        }
                    }
                }
            }
        }

        // Post-loop cleanup and final state update
        info!("Batch channel closed. Writing final state.");

        self.state_store.append_wal(&self.wal_item_done()).await?;

        let duration = start_time.elapsed();
        info!(duration = ?duration, "Consumer finished");

        self.send_final_report(&metrics).await;

        Ok(())
    }
}

impl LiveConsumer {
    pub async fn new(
        ctx: &Arc<Mutex<ItemContext>>,
        batch_rx: mpsc::Receiver<Batch>,
        shutdown_rx: Receiver<bool>,
        cancel: CancellationToken,
    ) -> Self {
        let (run_id, item_id, state_store, destination) = {
            let c = ctx.lock().await;
            (
                c.run_id.clone(),
                c.item_id.clone(),
                c.state.clone(),
                c.destination.clone(),
            )
        };

        let tables = match &destination.data_dest {
            DataDestination::Database(db) => db.data.lock().await.tables(),
        };

        // TODO: Part ID is hardcoded for now
        let part_id = "part-0".to_string();

        Self {
            ids: ItemId::new(run_id, item_id, part_id),
            state_store,
            destination,
            meta: tables,
            batch_rx,
            shutdown_rx,
            cancel,
        }
    }

    async fn process_batch(
        &mut self,
        batch: Batch,
        sink: &dyn Sink,
        metrics: &Metrics,
    ) -> Result<(), ConsumerError> {
        let start_time = Instant::now();

        let batch_id = batch.id.clone();
        let batch_rows = batch.rows.len();
        let next_cursor = batch.next.clone();

        info!(batch_id = %batch_id, rows = batch_rows, "Processing received batch");

        // For now we support only single destination table
        let meta = self.meta[0].clone();

        // Pre-write state management
        self.state_store
            .append_wal(&self.wal_batch_begin(batch_id.clone()))
            .await?;

        let write_checkpoint = self.build_checkpoint(
            "write".to_string(),
            batch_id.clone(),
            next_cursor.clone(),
            batch_rows as u64,
        );
        self.state_store.save_checkpoint(&write_checkpoint).await?;

        // Write data to destination
        self.write_batch(sink, &meta, &batch).await?;

        // Post-write state management
        self.state_store
            .append_wal(&self.wal_batch_commit(batch_id.clone()))
            .await?;

        let committed_checkpoint = self.build_checkpoint(
            "committed".to_string(),
            batch_id,
            next_cursor,
            batch_rows as u64,
        );
        self.state_store
            .save_checkpoint(&committed_checkpoint)
            .await?;

        // Metrics
        metrics.increment_bytes(batch.size_bytes() as u64).await;
        metrics.increment_records(batch_rows as u64).await;

        let duration = start_time.elapsed();
        info!(duration = ?duration, "Batch processed successfully");

        Ok(())
    }

    /// Handles the logic for writing a batch to the data sink,
    /// preferring the fast path if supported.
    async fn write_batch(
        &self,
        sink: &dyn Sink,
        meta: &TableMetadata,
        batch: &Batch,
    ) -> Result<(), ConsumerError> {
        let fast = sink.support_fast_path().await?;

        let write_result = if fast {
            info!("Using fast path for batch write.");

            // COPY -> MERGE
            sink.write_fast_path(meta, batch)
                .await
                .map_err(|e| ConsumerError::WriteBatch {
                    table: meta.name.clone(),
                    source: Box::new(e),
                })
        } else {
            info!("Using standard path for batch write.");

            // Fallback: multi-row UPSERT
            self.destination
                .write_batch(meta, &batch.rows)
                .await
                .map_err(|e| ConsumerError::WriteBatch {
                    table: meta.name.clone(),
                    source: Box::new(e),
                })
        };

        if let Err(ref e) = write_result {
            error!("Error writing batch to sink: {:?}", e);
        }

        write_result
    }

    async fn drain_pending_batches(
        &mut self,
        sink: &dyn Sink,
        metrics: &Metrics,
    ) -> Result<(), ConsumerError> {
        loop {
            match self.batch_rx.try_recv() {
                Ok(batch) => {
                    self.process_batch(batch, sink, metrics).await?;
                }
                Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => break,
            }
        }

        Ok(())
    }

    /// Helper to build a `BatchBeginWrite` WAL entry.
    fn wal_batch_begin(&self, batch_id: String) -> WalEntry {
        WalEntry::BatchBeginWrite {
            run_id: self.ids.run_id(),
            item_id: self.ids.item_id(),
            part_id: self.ids.part_id(),
            batch_id,
        }
    }

    /// Helper to build a `BatchCommit` WAL entry.
    fn wal_batch_commit(&self, batch_id: String) -> WalEntry {
        WalEntry::BatchCommit {
            run_id: self.ids.run_id(),
            item_id: self.ids.item_id(),
            part_id: self.ids.part_id(),
            batch_id,
        }
    }

    /// Helper to build an `ItemDone` WAL entry.
    fn wal_item_done(&self) -> WalEntry {
        WalEntry::ItemDone {
            run_id: self.ids.run_id(),
            item_id: self.ids.item_id(),
        }
    }

    /// Helper to build a new Checkpoint struct.
    fn build_checkpoint(
        &self,
        stage: String,
        batch_id: String,
        src_offset: Cursor,
        rows_done: u64,
    ) -> Checkpoint {
        Checkpoint {
            run_id: self.ids.run_id(),
            item_id: self.ids.item_id(),
            part_id: self.ids.part_id(),
            stage,
            src_offset,
            batch_id,
            rows_done,
            updated_at: chrono::Utc::now(),
        }
    }

    async fn send_final_report(&self, metrics: &Metrics) {
        let (records_processed, bytes_transferred) = metrics.get_metrics().await;
        let report = MetricsReport::new(records_processed, bytes_transferred, "succeeded".into());
        if let Err(e) = send_report(report.clone()).await {
            error!("Failed to send final report: {}", e);
            let report_json = serde_json::to_string(&report)
                .unwrap_or_else(|_| "Failed to serialize report".to_string());
            error!(
                "All attempts to send report failed. Final Report: {}",
                report_json
            );
        }
    }
}
