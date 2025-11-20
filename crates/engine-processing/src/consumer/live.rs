use crate::{
    cb::{CircuitBreaker, CircuitBreakerState},
    consumer::{DataConsumer, trigger::TriggerGuard},
    error::ConsumerError,
    item::ItemId,
    retry::{classify_db_error, classify_sink_error},
};
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
    retry::{RetryError, RetryPolicy},
    state::{
        StateStore,
        models::{Checkpoint, WalEntry},
    },
};
use futures::lock::Mutex;
use model::{pagination::cursor::Cursor, records::batch::Batch};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    sync::{mpsc, watch::Receiver},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

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
    pub heartbeat_interval: Duration,

    // Resilience
    breaker: CircuitBreaker,
    retry: RetryPolicy,
    metrics: Metrics,
    rows_done: u64,
}

#[async_trait]
impl DataConsumer for LiveConsumer {
    /// Main entry point for the consumer.
    /// Runs a loop to receive and process batches until the channel closes or cancellation is requested.
    async fn run(&mut self) -> Result<(), ConsumerError> {
        let start_time = Instant::now();
        let sink = self.destination.sink();
        let (heartbeat_shutdown, heartbeat_handle) = self.spawn_heartbeat();

        let result = async {
            // Guard to ensure triggers are restored on exit
            // TODO: Handle constraints more gracefully
            let _trigger_guard = TriggerGuard::new(&self.destination, &self.meta, false).await?;

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
                                self.process_batch(batch, sink.as_ref()).await?;
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
                                    .drain_pending_batches(sink.as_ref())
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

            Ok(())
        }
        .await;

        heartbeat_shutdown.cancel();
        if let Err(err) = heartbeat_handle.await {
            warn!(error = ?err, "Consumer heartbeat task did not shut down cleanly");
        }

        if result.is_ok() {
            self.send_final_report().await;
        }

        result
    }
}

impl LiveConsumer {
    pub async fn new(
        ctx: &Arc<Mutex<ItemContext>>,
        batch_rx: mpsc::Receiver<Batch>,
        shutdown_rx: Receiver<bool>,
        cancel: CancellationToken,
        metrics: Metrics,
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
        let state_store: Arc<dyn StateStore> = state_store;

        println!("Run ID: {}", run_id);
        println!("Item ID: {}", item_id);

        let tables = match &destination.data_dest {
            DataDestination::Database(db) => db.data.lock().await.tables(),
        };

        // TODO: Part ID is hardcoded for now
        let part_id = "part-0".to_string();
        let rows_done = Self::init_rows_done(&state_store, &run_id, &item_id, &part_id).await;

        Self {
            ids: ItemId::new(run_id, item_id, part_id),
            state_store,
            destination,
            meta: tables,
            batch_rx,
            shutdown_rx,
            cancel,
            heartbeat_interval: Duration::from_secs(30),
            breaker: CircuitBreaker::default_db(),
            retry: RetryPolicy::for_database(),
            metrics,
            rows_done,
        }
    }

    async fn process_batch(&mut self, batch: Batch, sink: &dyn Sink) -> Result<(), ConsumerError> {
        let start_time = Instant::now();

        let batch_id = batch.id.clone();
        let batch_rows = batch.rows.len();
        let next_cursor = batch.next.clone();
        let start_cursor = batch.cursor.clone();
        let cur_rows_done = self.rows_done;

        info!(batch_id = %batch_id, rows = batch_rows, "Processing received batch");

        // If no metadata is available, skip processing this batch
        if self.meta.is_empty() {
            warn!("No table metadata available for destination. Skipping batch.");
            return Ok(());
        }

        // For now we support only single destination table
        let meta = self.meta[0].clone();

        let write_checkpoint = self.build_checkpoint(
            "write",
            batch_id.clone(),
            start_cursor,
            Some(next_cursor.clone()),
            cur_rows_done,
        );
        self.state_store.save_checkpoint(&write_checkpoint).await?;

        // Write data to destination with retries + breaker protection
        self.ensure_write(sink, &meta, &batch).await?;

        // Post-write state management
        self.state_store
            .append_wal(&self.wal_batch_commit(batch_id.clone()))
            .await?;

        let committed_total = cur_rows_done + batch_rows as u64;
        let committed_checkpoint =
            self.build_checkpoint("committed", batch_id, next_cursor, None, committed_total);
        self.state_store
            .save_checkpoint(&committed_checkpoint)
            .await?;
        self.rows_done = committed_total;

        // Metrics
        self.metrics.increment_batches(1);
        self.metrics.increment_bytes(batch.size_bytes() as u64);
        self.metrics.increment_records(batch_rows as u64);

        let duration = start_time.elapsed();
        info!(duration = ?duration, "Batch processed successfully");

        Ok(())
    }

    /// Wraps the batch write with retries + circuit breaker.
    async fn ensure_write(
        &mut self,
        sink: &dyn Sink,
        meta: &TableMetadata,
        batch: &Batch,
    ) -> Result<(), ConsumerError> {
        loop {
            match self.write_batch(sink, meta, batch).await {
                Ok(()) => {
                    self.breaker.record_success();
                    return Ok(());
                }
                Err(err) => {
                    let delay = match self.handle_write_failure(&err).await {
                        Ok(delay) => delay,
                        Err(cb_err) => return Err(cb_err),
                    };
                    tokio::time::sleep(delay).await;
                }
            }
        }
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
        let fast = fast && !meta.primary_keys.is_empty();

        let write_result = if fast {
            info!("Using fast path for batch write.");

            let result = self
                .retry
                .run(
                    || async {
                        // COPY -> MERGE
                        sink.write_fast_path(meta, batch).await
                    },
                    classify_sink_error,
                )
                .await;
            match result {
                Ok(()) => Ok(()),
                Err(RetryError::Fatal(err)) => Err(ConsumerError::Sink(err)),
                Err(RetryError::AttemptsExceeded(err)) => Err(ConsumerError::RetriesExhausted(
                    format!("fast-path sink retries exhausted: {err}"),
                )),
            }
        } else {
            info!("Using standard path for batch write.");

            let result = self
                .retry
                .run(
                    || async move { self.destination.write_batch(meta, &batch.rows).await },
                    classify_db_error,
                )
                .await;

            match result {
                Ok(()) => Ok(()),
                Err(RetryError::Fatal(err)) => Err(ConsumerError::WriteBatch {
                    table: meta.name.clone(),
                    source: Box::new(err),
                }),
                Err(RetryError::AttemptsExceeded(err)) => Err(ConsumerError::RetriesExhausted(
                    format!("standard sink retries exhausted: {err}"),
                )),
            }
        };

        if let Err(ref e) = write_result {
            error!("Error writing batch to sink: {:?}", e);
        }

        write_result
    }

    async fn drain_pending_batches(&mut self, sink: &dyn Sink) -> Result<(), ConsumerError> {
        while let Ok(batch) = self.batch_rx.try_recv() {
            info!(
                batch_id = %batch.id,
                rows = batch.rows.len(),
                "Draining pending batch before shutdown"
            );
            self.process_batch(batch, sink).await?;
        }

        Ok(())
    }

    async fn handle_write_failure(
        &mut self,
        err: &ConsumerError,
    ) -> Result<Duration, ConsumerError> {
        let stage = "write";
        match self.breaker.record_failure() {
            CircuitBreakerState::RetryAfter(delay) => {
                warn!(
                    stage = stage,
                    failures = self.breaker.consecutive_failures(),
                    retry_in_ms = delay.as_millis(),
                    error = %err,
                    "Batch write failed; retrying after backoff"
                );
                Ok(delay)
            }
            CircuitBreakerState::Open => {
                error!(
                    stage = stage,
                    failures = self.breaker.consecutive_failures(),
                    error = %err,
                    "Circuit breaker opened for consumer; aborting item"
                );
                self.emit_breaker_wal(stage, err.to_string()).await?;
                Err(ConsumerError::CircuitBreakerOpen {
                    stage: stage.to_string(),
                    last_error: err.to_string(),
                })
            }
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

    async fn emit_breaker_wal(&self, stage: &str, last_error: String) -> Result<(), ConsumerError> {
        self.state_store
            .append_wal(&WalEntry::CircuitBreakerOpen {
                run_id: self.ids.run_id(),
                item_id: self.ids.item_id(),
                part_id: self.ids.part_id(),
                stage: stage.to_string(),
                failures: self.breaker.consecutive_failures(),
                last_error,
            })
            .await
            .map_err(ConsumerError::Unexpected)
    }

    /// Helper to build a new Checkpoint struct.
    fn build_checkpoint(
        &self,
        stage: &str,
        batch_id: String,
        src_offset: Cursor,
        pending_offset: Option<Cursor>,
        rows_done: u64,
    ) -> Checkpoint {
        Checkpoint {
            run_id: self.ids.run_id(),
            item_id: self.ids.item_id(),
            part_id: self.ids.part_id(),
            stage: stage.to_string(),
            src_offset,
            pending_offset,
            batch_id,
            rows_done,
            updated_at: chrono::Utc::now(),
        }
    }

    async fn init_rows_done(
        state_store: &Arc<dyn StateStore>,
        run_id: &str,
        item_id: &str,
        part_id: &str,
    ) -> u64 {
        match state_store.load_checkpoint(run_id, item_id, part_id).await {
            Ok(Some(cp)) => cp.rows_done,
            Ok(None) => 0,
            Err(err) => {
                warn!(
                    run_id = run_id,
                    item_id = item_id,
                    part_id = part_id,
                    error = %err,
                    "Failed to load checkpoint for initial rows_done; defaulting to 0"
                );
                0
            }
        }
    }

    fn spawn_heartbeat(&self) -> (CancellationToken, JoinHandle<()>) {
        let shutdown = CancellationToken::new();
        let shutdown_token = shutdown.clone();
        let run_id = self.ids.run_id();
        let item_id = self.ids.item_id();
        let part_id = self.ids.part_id();
        let state_store = self.state_store.clone();
        let cancel = self.cancel.clone();
        let interval = self.heartbeat_interval;

        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);

            loop {
                tokio::select! {
                     _ = ticker.tick() => {
                        if shutdown_token.is_cancelled() || cancel.is_cancelled() {
                            break;
                        }

                        if let Err(err) = state_store
                            .append_wal(&WalEntry::Heartbeat {
                                run_id: run_id.clone(),
                                item_id: item_id.clone(),
                                part_id: part_id.clone(),
                                at: chrono::Utc::now(),
                            })
                            .await
                        {
                            warn!(error = %err, "Failed to append consumer heartbeat");
                        }
                    }
                    _ = shutdown_token.cancelled() => break,
                    _ = cancel.cancelled() => break,
                }
            }
        });

        (shutdown, handle)
    }

    async fn send_final_report(&self) {
        let snapshot = self.metrics.snapshot();
        let report = MetricsReport::new(
            snapshot.records_processed,
            snapshot.bytes_transferred,
            snapshot.batches_processed,
            snapshot.failure_count,
            snapshot.retry_count,
            "succeeded".into(),
        );
        if let Err(e) = send_report(report.clone()).await {
            warn!("Failed to send final report: {}", e);
            let report_json = serde_json::to_string(&report)
                .unwrap_or_else(|_| "Failed to serialize report".to_string());
            warn!(
                "All attempts to send report failed. Final Report: {}",
                report_json
            );
        }
    }
}
