use super::TickAction;
use crate::actor::messages::ProducerMsg;
use crate::error::ActorError;
use engine_core::{event_bus::bus::EventBus, metrics::Metrics};
use engine_processing::{
    cb::{CircuitBreaker, CircuitBreakerState},
    error::ProducerError,
    producer::{Producer, ProducerStatus},
};
use model::events::migration::{MigrationEvent, ProducerMode};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

/// Encapsulates the state and logic of the running producer.
struct ProducerTask {
    producer: Producer,
    breaker: CircuitBreaker,
    metrics: Metrics,
    event_bus: EventBus,
    run_id: String,
    item_id: String,

    // Delta tracking
    last_batches_processed: u64,
    last_rows_skipped: u64,
    last_rows_failed: u64,
}

impl ProducerTask {
    fn new(
        producer: Producer,
        metrics: Metrics,
        event_bus: EventBus,
        run_id: String,
        item_id: String,
    ) -> Self {
        Self {
            producer,
            breaker: CircuitBreaker::default_db(),
            metrics,
            event_bus,
            run_id,
            item_id,
            last_batches_processed: 0,
            last_rows_skipped: 0,
            last_rows_failed: 0,
        }
    }

    /// Executes a single work unit.
    async fn tick(&mut self) -> TickAction {
        match self.producer.tick().await {
            Ok(status) => self.handle_success(status).await,
            Err(e) => self.handle_error(e).await,
        }
    }

    async fn handle_success(&mut self, status: ProducerStatus) -> TickAction {
        if self.breaker.consecutive_failures() > 0 {
            info!(
                run_id = %self.run_id,
                item_id = %self.item_id,
                "circuit breaker recovered"
            );
        }
        self.breaker.record_success();
        self.update_metrics().await;

        match status {
            ProducerStatus::Working => TickAction::Continue,
            ProducerStatus::Idle => TickAction::Idle,
            ProducerStatus::Finished => {
                info!(run_id = %self.run_id, item_id = %self.item_id, "producer finished");
                let _ = self.producer.stop().await;
                TickAction::Done
            }
        }
    }

    async fn handle_error(&mut self, e: ProducerError) -> TickAction {
        if e.is_shutdown() {
            info!(
                run_id = %self.run_id,
                item_id = %self.item_id,
                "consumer channel closed, producer stopping gracefully"
            );
            let _ = self.producer.stop().await;
            return TickAction::Done;
        }

        if e.is_fatal() {
            error!(
                run_id = %self.run_id,
                item_id = %self.item_id,
                error = %e,
                "fatal error, stopping migration immediately"
            );
            let _ = self.producer.stop().await;
            return TickAction::Failed(ActorError::Internal(e.to_string()));
        }

        self.metrics.increment_failures(1);
        error!(run_id = %self.run_id, item_id = %self.item_id, error = %e, "producer tick failed");

        match self.breaker.record_failure() {
            CircuitBreakerState::RetryAfter(delay) => {
                warn!(
                    run_id = %self.run_id,
                    item_id = %self.item_id,
                    delay_ms = delay.as_millis(),
                    failures = self.breaker.consecutive_failures(),
                    "circuit breaker backing off"
                );
                tokio::time::sleep(delay).await;
                TickAction::Continue
            }
            CircuitBreakerState::Open => {
                error!(
                    run_id = %self.run_id,
                    item_id = %self.item_id,
                    failures = self.breaker.consecutive_failures(),
                    "circuit breaker open, stopping producer"
                );
                let _ = self.producer.stop().await;
                TickAction::Done
            }
        }
    }

    async fn handle_stop(&mut self, run_id: String, item_id: String) -> TickAction {
        info!(run_id = %self.run_id, item_id = %self.item_id, "producer received stop");
        if let Err(e) = self.producer.stop().await {
            error!(run_id = %self.run_id, item_id = %self.item_id, error = %e, "producer stop failed");
            return TickAction::Failed(ActorError::Internal(e.to_string()));
        }

        let rows_produced = self.producer.rows_produced();
        self.event_bus
            .publish(MigrationEvent::ProducerStopped {
                run_id,
                item_id,
                timestamp: chrono::Utc::now(),
                rows_produced,
            })
            .await;

        TickAction::Done
    }

    async fn update_metrics(&mut self) {
        let current_batches = self.producer.batches_processed();
        let current_skipped = self.producer.total_rows_skipped();
        let current_failed = self.producer.total_rows_failed();

        if current_batches > self.last_batches_processed {
            let delta = current_batches - self.last_batches_processed;
            self.metrics.increment_batches(delta);
            self.last_batches_processed = current_batches;

            for _ in 0..delta {
                self.event_bus
                    .publish(MigrationEvent::BatchRead {
                        run_id: self.run_id.clone(),
                        item_id: self.item_id.clone(),
                        batch_id: format!("batch-{}", current_batches),
                        row_count: 0,
                        timestamp: chrono::Utc::now(),
                    })
                    .await;
            }
        }

        if current_skipped > self.last_rows_skipped {
            self.metrics
                .increment_rows_skipped(current_skipped - self.last_rows_skipped);
            self.last_rows_skipped = current_skipped;
        }

        if current_failed > self.last_rows_failed {
            self.metrics
                .increment_rows_failed(current_failed - self.last_rows_failed);
            self.last_rows_failed = current_failed;
        }
    }
}

/// Runs the producer loop as a standalone async task.
pub async fn run_producer(
    mut producer: Producer,
    mut rx: mpsc::Receiver<ProducerMsg>,
    cancel_token: CancellationToken,
    event_bus: EventBus,
    metrics: Metrics,
) -> Result<(), ActorError> {
    // Wait for start signal
    let (run_id, item_id) =
        match wait_for_start(&mut producer, &mut rx, &cancel_token, &event_bus).await? {
            Some(ids) => ids,
            None => return Ok(()), // Cancelled or Stopped before starting
        };

    let mut task = ProducerTask::new(producer, metrics, event_bus, run_id, item_id);
    let idle_delay = Duration::from_millis(500);
    let mut tick_interval = tokio::time::interval(Duration::from_millis(1));
    tick_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // Main loop: tick producer and handle messages
    loop {
        tokio::select! {
            _ = tick_interval.tick() => {
                match task.tick().await {
                    TickAction::Continue => tick_interval.reset_immediately(),
                    TickAction::Idle => {
                        tick_interval = tokio::time::interval(idle_delay);
                        tick_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    }
                    TickAction::Done => return Ok(()),
                    TickAction::Failed(e) => return Err(e),
                }
            }
            msg = rx.recv() => match msg {
                Some(ProducerMsg::Stop { run_id, item_id }) => {
                    return match task.handle_stop(run_id, item_id).await {
                        TickAction::Done => Ok(()),
                        TickAction::Failed(e) => Err(e),
                        _ => Ok(()),
                    };
                }
                Some(_) => {}
                None => {
                    info!(run_id = %task.run_id, item_id = %task.item_id, "producer mailbox closed, stopping");
                    let _ = task.producer.stop().await;
                    return Ok(());
                }
            },
            _ = cancel_token.cancelled() => {
                info!(run_id = %task.run_id, item_id = %task.item_id, "producer stopping after cancellation");
                let _ = task.producer.stop().await;
                return Ok(());
            }
        }
    }
}

async fn wait_for_start(
    producer: &mut Producer,
    rx: &mut mpsc::Receiver<ProducerMsg>,
    cancel_token: &CancellationToken,
    event_bus: &EventBus,
) -> Result<Option<(String, String)>, ActorError> {
    tokio::select! {
        msg = rx.recv() => match msg {
            Some(ProducerMsg::StartSnapshot { run_id, item_id }) => {
                start_snapshot(producer, event_bus, &run_id, &item_id).await?;
                Ok(Some((run_id, item_id)))
            }
            Some(ProducerMsg::StartCdc { run_id, item_id }) => {
                start_cdc(producer, event_bus, &run_id, &item_id).await?;
                Ok(Some((run_id, item_id)))
            }
            Some(ProducerMsg::Stop { .. }) | None => {
                info!("producer stopping before start");
                Ok(None)
            }
        },
        _ = cancel_token.cancelled() => {
            info!("producer cancelled before start");
            Ok(None)
        }
    }
}

async fn start_snapshot(
    producer: &mut Producer,
    event_bus: &EventBus,
    run_id: &str,
    item_id: &str,
) -> Result<(), ActorError> {
    if let Err(e) = producer.resume(run_id, item_id, "part-0").await {
        error!(run_id = %run_id, item_id = %item_id, error = %e, "failed to resume producer state");
        return Err(ActorError::Internal(e.to_string()));
    }
    if let Err(e) = producer.start_snapshot().await {
        error!(run_id = %run_id, item_id = %item_id, error = %e, "failed to start snapshot");
        return Err(ActorError::Internal(e.to_string()));
    }

    event_bus
        .publish(MigrationEvent::SnapshotStarted {
            run_id: run_id.to_owned(),
            item_id: item_id.to_owned(),
            timestamp: chrono::Utc::now(),
            estimated_rows: None,
        })
        .await;
    event_bus
        .publish(MigrationEvent::ProducerStarted {
            run_id: run_id.to_owned(),
            item_id: item_id.to_owned(),
            mode: ProducerMode::Snapshot,
            timestamp: chrono::Utc::now(),
        })
        .await;

    Ok(())
}

async fn start_cdc(
    producer: &mut Producer,
    event_bus: &EventBus,
    run_id: &str,
    item_id: &str,
) -> Result<(), ActorError> {
    if let Err(e) = producer.start_cdc().await {
        error!(run_id = %run_id, item_id = %item_id, error = %e, "failed to start CDC");
        return Err(ActorError::Internal(e.to_string()));
    }

    event_bus
        .publish(MigrationEvent::CdcStarted {
            run_id: run_id.to_owned(),
            item_id: item_id.to_owned(),
            timestamp: chrono::Utc::now(),
            starting_position: None,
        })
        .await;
    event_bus
        .publish(MigrationEvent::ProducerStarted {
            run_id: run_id.to_owned(),
            item_id: item_id.to_owned(),
            mode: ProducerMode::Cdc,
            timestamp: chrono::Utc::now(),
        })
        .await;

    Ok(())
}
