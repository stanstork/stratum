use super::TickAction;
use crate::actor::messages::ConsumerMsg;
use crate::error::ActorError;
use engine_core::{event_bus::bus::EventBus, metrics::Metrics};
use engine_processing::{
    cb::{CircuitBreaker, CircuitBreakerState},
    consumer::{Consumer, ConsumerStatus},
    error::ConsumerError,
};
use model::events::migration::MigrationEvent;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

/// Encapsulates the state and logic of the running consumer.
struct ConsumerTask {
    consumer: Consumer,
    breaker: CircuitBreaker,
    metrics: Metrics,
    event_bus: EventBus,
    run_id: String,
    item_id: String,

    // Progress tracking
    start_time: Instant,
    last_progress_report: Instant,
    progress_interval: Duration,

    last_log_report: Instant,
    log_interval: Duration,
}

impl ConsumerTask {
    fn new(
        consumer: Consumer,
        metrics: Metrics,
        event_bus: EventBus,
        run_id: String,
        item_id: String,
    ) -> Self {
        let now = Instant::now();
        Self {
            consumer,
            breaker: CircuitBreaker::default_db(),
            metrics,
            event_bus,
            run_id,
            item_id,
            start_time: now,
            last_progress_report: now,
            progress_interval: Duration::from_millis(500),
            last_log_report: now,
            log_interval: Duration::from_secs(5),
        }
    }

    async fn tick(&mut self) -> TickAction {
        match self.consumer.tick().await {
            Ok(status) => self.handle_success(status).await,
            Err(e) => self.handle_error(e).await,
        }
    }

    async fn handle_success(&mut self, status: ConsumerStatus) -> TickAction {
        if self.breaker.consecutive_failures() > 0 {
            info!(
                run_id = %self.run_id,
                item_id = %self.item_id,
                "circuit breaker recovered"
            );
        }
        self.breaker.record_success();
        self.report_progress().await;

        match status {
            ConsumerStatus::Working => TickAction::Continue,
            ConsumerStatus::Idle => TickAction::Idle,
            ConsumerStatus::Finished => {
                info!(run_id = %self.run_id, item_id = %self.item_id, "consumer finished");
                if let Err(e) = self.consumer.finalize().await {
                    error!(run_id = %self.run_id, item_id = %self.item_id, error = %e, "consumer finalize failed");
                    let _ = self.consumer.stop().await;
                    return TickAction::Failed(ActorError::Internal(e.to_string()));
                }
                let _ = self.consumer.stop().await;
                TickAction::Done
            }
        }
    }

    async fn handle_error(&mut self, e: ConsumerError) -> TickAction {
        self.metrics.increment_failures(1);
        error!(run_id = %self.run_id, item_id = %self.item_id, error = %e, "consumer tick failed");

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
                    "circuit breaker open, stopping consumer"
                );
                let _ = self.consumer.stop().await;
                TickAction::Done
            }
        }
    }

    async fn handle_stop(
        &mut self,
        run_id: String,
        item_id: String,
        part_id: String,
    ) -> TickAction {
        info!(run_id = %self.run_id, item_id = %self.item_id, "consumer received stop");
        if let Err(e) = self.consumer.stop().await {
            error!(run_id = %self.run_id, item_id = %self.item_id, error = %e, "consumer stop failed");
            return TickAction::Failed(ActorError::Internal(e.to_string()));
        }

        let rows_written = self.consumer.rows_written();
        self.event_bus
            .publish(MigrationEvent::ConsumerStopped {
                run_id,
                item_id,
                timestamp: chrono::Utc::now(),
                part_id,
                rows_written,
            })
            .await;

        TickAction::Done
    }

    async fn report_progress(&mut self) {
        let now = std::time::Instant::now();
        if now.duration_since(self.last_progress_report) < self.progress_interval {
            return;
        }

        let snapshot = self.metrics.snapshot();
        let elapsed = now.duration_since(self.start_time).as_secs_f64();
        let rows_per_second = if elapsed > 0.0 {
            snapshot.records_processed as f64 / elapsed
        } else {
            0.0
        };

        self.event_bus
            .publish(MigrationEvent::Progress {
                run_id: self.run_id.clone(),
                item_id: self.item_id.clone(),
                rows_processed: snapshot.records_processed,
                rows_skipped: snapshot.rows_skipped,
                rows_failed: snapshot.rows_failed,
                bytes_transferred: snapshot.bytes_transferred,
                rows_per_second,
                timestamp: chrono::Utc::now(),
            })
            .await;
        self.last_progress_report = now;

        // Human-readable heartbeat, throttled well below the event cadence so
        // long migrations show progress in headless/file logs.
        if now.duration_since(self.last_log_report) >= self.log_interval {
            info!(
                run_id = %self.run_id,
                item_id = %self.item_id,
                rows = snapshot.records_processed,
                rows_per_sec = %format!("{:.0}", rows_per_second),
                skipped = snapshot.rows_skipped,
                failed = snapshot.rows_failed,
                "migration progress"
            );
            self.last_log_report = now;
        }
    }
}

/// Runs the consumer loop as a standalone async task.
pub async fn run_consumer(
    mut consumer: Consumer,
    mut rx: mpsc::Receiver<ConsumerMsg>,
    cancel_token: CancellationToken,
    event_bus: EventBus,
    metrics: Metrics,
) -> Result<(), ActorError> {
    // Wait for start signal
    let (run_id, item_id) =
        match wait_for_start(&mut consumer, &mut rx, &cancel_token, &event_bus).await? {
            Some(ids) => ids,
            None => return Ok(()), // Cancelled or Stopped before starting
        };

    let mut task = ConsumerTask::new(consumer, metrics, event_bus, run_id, item_id);
    let idle_delay = Duration::from_millis(100);
    let mut tick_interval = tokio::time::interval(Duration::from_millis(1));
    tick_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // Main loop: tick consumer and handle messages
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
                Some(ConsumerMsg::Flush { run_id, item_id }) => {
                    info!(run_id = %run_id, item_id = %item_id, "flushing consumer");
                    tick_interval.reset_immediately();
                }
                Some(ConsumerMsg::Stop { run_id, item_id, part_id }) => {
                    return match task.handle_stop(run_id, item_id, part_id).await {
                        TickAction::Done => Ok(()),
                        TickAction::Failed(e) => Err(e),
                        _ => Ok(()),
                    };
                }
                Some(_) => {}
                None => {
                    info!(run_id = %task.run_id, item_id = %task.item_id, "consumer mailbox closed, stopping");
                    let _ = task.consumer.stop().await;
                    return Ok(());
                }
            },
            _ = cancel_token.cancelled() => {
                info!(run_id = %task.run_id, item_id = %task.item_id, "consumer stopping after cancellation");
                let _ = task.consumer.stop().await;
                return Ok(());
            }
        }
    }
}

async fn wait_for_start(
    consumer: &mut Consumer,
    rx: &mut mpsc::Receiver<ConsumerMsg>,
    cancel_token: &CancellationToken,
    event_bus: &EventBus,
) -> Result<Option<(String, String)>, ActorError> {
    loop {
        tokio::select! {
            msg = rx.recv() => match msg {
                Some(ConsumerMsg::Start { run_id, item_id, part_id }) => {
                    if let Err(e) = consumer.start().await {
                        error!(run_id = %run_id, item_id = %item_id, error = %e, "failed to start consumer");
                        return Err(ActorError::Internal(e.to_string()));
                    }
                    if let Err(e) = consumer.resume(&run_id, &item_id, &part_id).await {
                        warn!(run_id = %run_id, item_id = %item_id, error = %e, "failed to resume consumer state");
                    }

                    event_bus
                        .publish(MigrationEvent::ConsumerStarted {
                            run_id: run_id.clone(),
                            item_id: item_id.clone(),
                            part_id,
                            timestamp: chrono::Utc::now(),
                        })
                        .await;

                    return Ok(Some((run_id, item_id)));
                }
                Some(ConsumerMsg::Stop { .. }) | None => {
                    info!("consumer stopping before start");
                    return Ok(None);
                }
                Some(_) => {}
            },
            _ = cancel_token.cancelled() => {
                info!("consumer cancelled before start");
                return Ok(None);
            }
        }
    }
}
