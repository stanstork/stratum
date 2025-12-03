use crate::{
    actor::{Actor, ActorContext, ActorRef, messages::ProducerMsg},
    error::ActorError,
};
use async_trait::async_trait;
use engine_core::{event_bus::bus::EventBus, metrics::Metrics};
use engine_processing::{
    cb::{CircuitBreaker, CircuitBreakerState},
    producer::{DataProducer, ProducerStatus},
};
use model::events::migration::{MigrationEvent, ProducerMode};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TickResponse {
    /// Schedule another tick immediately
    ScheduleImmediate,

    /// Schedule another tick after a delay
    ScheduleDelayed(Duration),

    /// No more ticks needed (finished or stopped)
    NoMoreTicks,
}

pub struct ProducerActor {
    producer: Box<dyn DataProducer + Send + 'static>,

    // Control state
    running: bool,
    cancel_token: CancellationToken,
    mode: Option<ProducerMode>,

    // Resilience
    breaker: CircuitBreaker,
    metrics: Metrics,

    // Configuration
    idle_delay: Duration,

    // Self-reference for scheduling ticks
    actor_ref: Option<ActorRef<ProducerMsg>>,

    // EventBus for publishing migration events
    event_bus: Option<EventBus>,
}

impl ProducerActor {
    pub fn new(
        producer: Box<dyn DataProducer + Send + 'static>,
        cancel_token: CancellationToken,
        metrics: Metrics,
    ) -> Self {
        Self {
            producer,
            running: false,
            cancel_token,
            mode: None,
            breaker: CircuitBreaker::default_db(),
            metrics,
            idle_delay: Duration::from_millis(500), // Default idle delay
            actor_ref: None,
            event_bus: None,
        }
    }

    pub fn set_actor_ref(&mut self, actor_ref: ActorRef<ProducerMsg>) {
        self.actor_ref = Some(actor_ref);
    }

    pub fn set_event_bus(&mut self, event_bus: EventBus) {
        self.event_bus = Some(event_bus);
    }

    fn next_action(&self, status: ProducerStatus) -> TickResponse {
        match status {
            ProducerStatus::Working => TickResponse::ScheduleImmediate,
            ProducerStatus::Idle => TickResponse::ScheduleDelayed(self.idle_delay),
            ProducerStatus::Finished => TickResponse::NoMoreTicks,
        }
    }

    async fn handle_tick_error(
        &mut self,
        error: impl std::fmt::Display,
    ) -> Result<TickResponse, ActorError> {
        self.metrics.increment_failures(1);
        error!(error = %error, "Producer tick failed");

        match self.breaker.record_failure() {
            CircuitBreakerState::RetryAfter(delay) => {
                warn!(
                    delay_ms = delay.as_millis(),
                    failures = self.breaker.consecutive_failures(),
                    "Circuit breaker: backing off"
                );
                tokio::time::sleep(delay).await;
                Ok(TickResponse::ScheduleImmediate)
            }
            CircuitBreakerState::Open => {
                error!(
                    failures = self.breaker.consecutive_failures(),
                    "Circuit breaker open, stopping producer"
                );
                self.running = false;
                Ok(TickResponse::NoMoreTicks)
            }
        }
    }
}

#[async_trait]
impl Actor<ProducerMsg> for ProducerActor {
    async fn on_start(&mut self, _ctx: &ActorContext) -> Result<(), ActorError> {
        Ok(())
    }

    async fn handle(&mut self, msg: ProducerMsg, ctx: &ActorContext) -> Result<(), ActorError> {
        match msg {
            ProducerMsg::SetActorRef(actor_ref) => {
                self.set_actor_ref(actor_ref);
                Ok(())
            }

            ProducerMsg::SetEventBus(event_bus) => {
                self.set_event_bus(event_bus);
                Ok(())
            }

            ProducerMsg::StartSnapshot { run_id, item_id } => {
                if let Err(e) = self.producer.resume(&run_id, &item_id, "part-0").await {
                    error!("Failed to resume producer state: {}", e);
                    return Err(ActorError::Internal(e.to_string()));
                }

                if let Err(e) = self.producer.start_snapshot().await {
                    error!("Failed to start snapshot: {}", e);
                    return Err(ActorError::Internal(e.to_string()));
                }

                self.running = true;
                self.mode = Some(ProducerMode::Snapshot);

                if let Some(ref event_bus) = self.event_bus {
                    event_bus
                        .publish(MigrationEvent::SnapshotStarted {
                            run_id: run_id.clone(),
                            item_id: item_id.clone(),
                            timestamp: chrono::Utc::now(),
                            estimated_rows: None,
                        })
                        .await;
                    event_bus
                        .publish(MigrationEvent::ProducerStarted {
                            run_id: run_id.clone(),
                            item_id: item_id.clone(),
                            mode: ProducerMode::Snapshot,
                            timestamp: chrono::Utc::now(),
                        })
                        .await;
                }

                // Send the first tick to start the processing loop
                if let Some(ref actor_ref) = self.actor_ref {
                    actor_ref.try_send(ProducerMsg::Tick)?;
                }

                Ok(())
            }

            ProducerMsg::StartCdc { run_id, item_id } => {
                if let Err(e) = self.producer.start_cdc().await {
                    error!("Failed to start CDC: {}", e);
                    return Err(ActorError::Internal(e.to_string()));
                }
                self.running = true;
                self.mode = Some(ProducerMode::Cdc);

                if let Some(ref event_bus) = self.event_bus {
                    event_bus
                        .publish(MigrationEvent::CdcStarted {
                            run_id: run_id.clone(),
                            item_id: item_id.clone(),
                            timestamp: chrono::Utc::now(),
                            starting_position: None,
                        })
                        .await;
                    event_bus
                        .publish(MigrationEvent::ProducerStarted {
                            run_id: run_id.clone(),
                            item_id: item_id.clone(),
                            mode: ProducerMode::Cdc,
                            timestamp: chrono::Utc::now(),
                        })
                        .await;
                }

                // Send the first tick to start the processing loop
                if let Some(ref actor_ref) = self.actor_ref {
                    actor_ref.try_send(ProducerMsg::Tick)?;
                }

                Ok(())
            }

            ProducerMsg::Tick => {
                // Check if shutdown requested (but allow current tick to complete)
                let shutdown_requested = self.cancel_token.is_cancelled();

                if !self.running {
                    info!(actor = ctx.name(), "Producer stopping");
                    let _ = self.producer.stop().await;
                    // Drop self-reference to allow actor termination
                    self.actor_ref = None;
                    return Ok(());
                }

                // Process one tick to complete any in-flight batch
                let response = match self.producer.tick().await {
                    Ok(status) => {
                        self.breaker.record_success();

                        if shutdown_requested {
                            info!(
                                actor = ctx.name(),
                                "Producer stopping after completing in-flight work"
                            );
                            self.running = false;
                            let _ = self.producer.stop().await;
                            self.actor_ref = None;
                            return Ok(());
                        }

                        self.next_action(status)
                    }
                    Err(e) => {
                        // Check if error is due to channel being closed (consumer finished)
                        let error_msg = e.to_string();
                        if error_msg.contains("channel closed")
                            || error_msg.contains("Channel already closed")
                        {
                            info!(
                                actor = ctx.name(),
                                "Consumer channel closed, producer stopping"
                            );
                            self.running = false;
                            let _ = self.producer.stop().await;
                            self.actor_ref = None;
                            return Ok(());
                        }

                        self.handle_tick_error(e).await?
                    }
                };

                match response {
                    TickResponse::ScheduleImmediate => {
                        if let Some(ref actor_ref) = self.actor_ref {
                            let actor_ref = actor_ref.clone();
                            tokio::spawn(async move {
                                if let Err(e) = actor_ref.send(ProducerMsg::Tick).await {
                                    error!(error = ?e, "Failed to schedule immediate tick");
                                }
                            });
                        }
                    }
                    TickResponse::ScheduleDelayed(delay) => {
                        info!(delay_ms = delay.as_millis(), "Producer idle");
                        if let Some(ref actor_ref) = self.actor_ref {
                            let actor_ref = actor_ref.clone();
                            tokio::spawn(async move {
                                tokio::time::sleep(delay).await;
                                if let Err(e) = actor_ref.send(ProducerMsg::Tick).await {
                                    error!(error = ?e, "Failed to schedule delayed tick");
                                }
                            });
                        }
                    }
                    TickResponse::NoMoreTicks => {
                        info!(
                            "Producer finished - dropping self-reference to allow actor termination"
                        );
                        self.running = false;
                        let _ = self.producer.stop().await;

                        // Drop self-reference so the mailbox can close and actor can terminate
                        self.actor_ref = None;
                    }
                }

                Ok(())
            }

            ProducerMsg::Stop { run_id, item_id } => {
                self.running = false;
                if let Err(e) = self.producer.stop().await {
                    error!(error = %e, "Producer stop failed");
                    return Err(ActorError::Internal(e.to_string()));
                }

                if let Some(ref event_bus) = self.event_bus {
                    let rows_produced = self.producer.rows_produced();
                    event_bus
                        .publish(MigrationEvent::ProducerStopped {
                            run_id,
                            item_id,
                            timestamp: chrono::Utc::now(),
                            rows_produced,
                        })
                        .await;
                }

                Ok(())
            }
        }
    }

    async fn on_stop(&mut self, _ctx: &ActorContext) -> Result<(), ActorError> {
        info!("Producer actor stopping.");
        if let Err(e) = self.producer.stop().await {
            error!(?e, "Producer stop failed");
            return Err(ActorError::Internal(e.to_string()));
        }
        Ok(())
    }
}
