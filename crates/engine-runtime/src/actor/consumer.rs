use crate::{
    actor::{Actor, ActorContext, ActorRef, messages::ConsumerMsg},
    error::ActorError,
};
use async_trait::async_trait;
use engine_core::{event_bus::bus::EventBus, metrics::Metrics};
use engine_processing::{
    cb::{CircuitBreaker, CircuitBreakerState},
    consumer::{ConsumerStatus, DataConsumer},
};
use model::events::migration::MigrationEvent;
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

pub struct ConsumerActor {
    consumer: Box<dyn DataConsumer + Send + 'static>,

    // Control state
    running: bool,
    cancel_token: CancellationToken,

    // Resilience
    breaker: CircuitBreaker,
    metrics: Metrics,

    // Configuration
    idle_delay: Duration,

    // Self-reference for scheduling ticks
    actor_ref: Option<ActorRef<ConsumerMsg>>,

    // EventBus for publishing migration events
    event_bus: Option<EventBus>,
}

impl ConsumerActor {
    pub fn new(
        consumer: Box<dyn DataConsumer + Send + 'static>,
        cancel_token: CancellationToken,
        metrics: Metrics,
    ) -> Self {
        Self {
            consumer,
            running: false,
            cancel_token,
            breaker: CircuitBreaker::default_db(),
            metrics,
            idle_delay: Duration::from_millis(100), // Default idle delay
            actor_ref: None,
            event_bus: None,
        }
    }

    pub fn set_actor_ref(&mut self, actor_ref: ActorRef<ConsumerMsg>) {
        self.actor_ref = Some(actor_ref);
    }

    pub fn set_event_bus(&mut self, event_bus: EventBus) {
        self.event_bus = Some(event_bus);
    }

    fn next_action(&self, status: ConsumerStatus) -> TickResponse {
        match status {
            ConsumerStatus::Working => TickResponse::ScheduleImmediate,
            ConsumerStatus::Idle => TickResponse::ScheduleDelayed(self.idle_delay),
            ConsumerStatus::Finished => TickResponse::NoMoreTicks,
        }
    }

    async fn handle_tick_error(
        &mut self,
        error: impl std::fmt::Display,
    ) -> Result<TickResponse, ActorError> {
        self.metrics.increment_failures(1);
        error!(error = %error, "Consumer tick failed");

        match self.breaker.record_failure() {
            CircuitBreakerState::RetryAfter(delay) => {
                warn!(
                    delay_ms = delay.as_millis(),
                    failures = self.breaker.consecutive_failures(),
                    "Circuit breaker: backing off"
                );
                tokio::time::sleep(delay).await;

                // Tell runtime to schedule another tick
                Ok(TickResponse::ScheduleImmediate)
            }
            CircuitBreakerState::Open => {
                error!(
                    failures = self.breaker.consecutive_failures(),
                    "Circuit breaker open, stopping consumer"
                );
                self.running = false;
                Ok(TickResponse::NoMoreTicks)
            }
        }
    }
}

#[async_trait]
impl Actor<ConsumerMsg> for ConsumerActor {
    async fn on_start(&mut self, _ctx: &ActorContext) -> Result<(), ActorError> {
        Ok(())
    }

    async fn handle(&mut self, msg: ConsumerMsg, ctx: &ActorContext) -> Result<(), ActorError> {
        match msg {
            ConsumerMsg::SetActorRef(actor_ref) => {
                self.set_actor_ref(actor_ref);
                Ok(())
            }

            ConsumerMsg::SetEventBus(event_bus) => {
                self.set_event_bus(event_bus);
                Ok(())
            }

            ConsumerMsg::Start {
                run_id,
                item_id,
                part_id,
            } => {
                if let Err(e) = self.consumer.start().await {
                    error!(error = %e, "Failed to start consumer");
                    return Err(ActorError::Internal(e.to_string()));
                }

                // Try to resume from checkpoint
                if let Err(e) = self.consumer.resume(&run_id, &item_id, &part_id).await {
                    warn!(error = %e, "Failed to resume consumer state");
                }

                self.running = true;

                if let Some(ref event_bus) = self.event_bus {
                    event_bus
                        .publish(MigrationEvent::ConsumerStarted {
                            run_id: run_id.clone(),
                            item_id: item_id.clone(),
                            part_id: part_id.clone(),
                            timestamp: chrono::Utc::now(),
                        })
                        .await;
                }

                // Send the first tick to start the processing loop
                if let Some(ref actor_ref) = self.actor_ref {
                    actor_ref.try_send(ConsumerMsg::Tick)?;
                }

                Ok(())
            }

            ConsumerMsg::Tick => {
                // Check if shutdown requested (but allow current tick to complete)
                let shutdown_requested = self.cancel_token.is_cancelled();

                if !self.running {
                    info!(actor = ctx.name(), "Consumer stopping");
                    let _ = self.consumer.stop().await;
                    // Drop self-reference to allow actor termination
                    self.actor_ref = None;
                    return Ok(());
                }

                // Process one tick to complete any in-flight batch
                let response = match self.consumer.tick().await {
                    Ok(status) => {
                        self.breaker.record_success();

                        if shutdown_requested {
                            info!(
                                actor = ctx.name(),
                                "Consumer stopping after completing in-flight work"
                            );
                            self.running = false;
                            let _ = self.consumer.stop().await;
                            self.actor_ref = None;
                            return Ok(());
                        }

                        self.next_action(status)
                    }
                    Err(e) => self.handle_tick_error(e).await?,
                };

                match response {
                    TickResponse::ScheduleImmediate => {
                        if let Some(ref actor_ref) = self.actor_ref {
                            let actor_ref = actor_ref.clone();
                            tokio::spawn(async move {
                                if let Err(e) = actor_ref.send(ConsumerMsg::Tick).await {
                                    error!(error = ?e, "Failed to schedule immediate tick");
                                }
                            });
                        }
                    }
                    TickResponse::ScheduleDelayed(delay) => {
                        info!(delay_ms = delay.as_millis(), "Consumer idle");
                        if let Some(ref actor_ref) = self.actor_ref {
                            let actor_ref = actor_ref.clone();
                            tokio::spawn(async move {
                                tokio::time::sleep(delay).await;
                                if let Err(e) = actor_ref.send(ConsumerMsg::Tick).await {
                                    error!(error = ?e, "Failed to schedule delayed tick");
                                }
                            });
                        }
                    }
                    TickResponse::NoMoreTicks => {
                        info!(
                            "Consumer finished - dropping self-reference to allow actor termination"
                        );
                        self.running = false;

                        // Drop self-reference so the mailbox can close and actor can terminates
                        self.actor_ref = None;
                    }
                }

                Ok(())
            }

            ConsumerMsg::Flush { run_id, item_id } => {
                info!(
                    actor = ctx.name(),
                    run_id = %run_id,
                    item_id = %item_id,
                    "Flushing consumer - processing remaining batches"
                );

                // If not already running, start ticking to process remaining batches
                if !self.running {
                    self.running = true;
                    if let Some(ref actor_ref) = self.actor_ref {
                        actor_ref.try_send(ConsumerMsg::Tick)?;
                    }
                }

                Ok(())
            }

            ConsumerMsg::Stop {
                run_id,
                item_id,
                part_id,
            } => {
                self.running = false;

                if let Err(e) = self.consumer.stop().await {
                    error!(error = %e, "Consumer stop failed");
                    return Err(ActorError::Internal(e.to_string()));
                }

                if let Some(ref event_bus) = self.event_bus {
                    let rows_written = self.consumer.rows_written();
                    event_bus
                        .publish(MigrationEvent::ConsumerStopped {
                            run_id,
                            item_id,
                            timestamp: chrono::Utc::now(),
                            part_id,
                            rows_written,
                        })
                        .await;
                }

                Ok(())
            }
        }
    }

    async fn on_stop(&mut self, _ctx: &ActorContext) -> Result<(), ActorError> {
        info!("Consumer actor stopping.");
        if let Err(e) = self.consumer.stop().await {
            error!(?e, "Consumer stop failed");
            return Err(ActorError::Internal(e.to_string()));
        }
        Ok(())
    }
}
