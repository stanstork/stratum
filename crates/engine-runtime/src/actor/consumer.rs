use crate::{
    actor::{
        actor::{Actor, ActorContext, ActorRef},
        messages::ConsumerMsg,
    },
    error::ActorError,
};
use async_trait::async_trait;
use engine_core::metrics::Metrics;
use engine_processing::{
    cb::{CircuitBreaker, CircuitBreakerState},
    consumer::{ConsumerStatus, DataConsumer},
};
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

pub struct ConsumerActor<C>
where
    C: DataConsumer + Send + 'static,
{
    consumer: C,

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
}

impl<C> ConsumerActor<C>
where
    C: DataConsumer + Send + 'static,
{
    pub fn new(consumer: C, cancel_token: CancellationToken, metrics: Metrics) -> Self {
        Self {
            consumer,
            running: false,
            cancel_token,
            breaker: CircuitBreaker::default_db(),
            metrics,
            idle_delay: Duration::from_millis(100), // Default idle delay
            actor_ref: None,
        }
    }

    /// This must be called after the actor is spawned.
    pub fn set_actor_ref(&mut self, actor_ref: ActorRef<ConsumerMsg>) {
        self.actor_ref = Some(actor_ref);
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
impl<C> Actor<ConsumerMsg> for ConsumerActor<C>
where
    C: DataConsumer + Send + 'static,
{
    async fn on_start(&mut self, ctx: &ActorContext) -> Result<(), ActorError> {
        info!(actor = ctx.name(), "Consumer actor started");
        Ok(())
    }

    async fn handle(&mut self, msg: ConsumerMsg, ctx: &ActorContext) -> Result<(), ActorError> {
        match msg {
            ConsumerMsg::SetActorRef(actor_ref) => {
                info!(
                    actor = ctx.name(),
                    "Setting actor reference for tick scheduling"
                );
                self.set_actor_ref(actor_ref);
                Ok(())
            }

            ConsumerMsg::Start {
                run_id,
                item_id,
                part_id,
            } => {
                info!(
                    actor = ctx.name(),
                    run_id = %run_id,
                    item_id = %item_id,
                    part_id = %part_id,
                    "Starting consumer"
                );

                if let Err(e) = self.consumer.start().await {
                    error!(error = %e, "Failed to start consumer");
                    return Err(ActorError::Internal(e.to_string()));
                }

                // Try to resume from checkpoint
                if let Err(e) = self.consumer.resume(&run_id, &item_id, &part_id).await {
                    warn!(error = %e, "Failed to resume consumer state");
                }

                self.running = true;

                info!("Consumer started, sending initial tick");

                // Send the first tick to start the processing loop
                if let Some(ref actor_ref) = self.actor_ref {
                    actor_ref.try_send(ConsumerMsg::Tick)?;
                }

                Ok(())
            }

            ConsumerMsg::Tick => {
                // Stop if not running or cancelled
                if !self.running || self.cancel_token.is_cancelled() {
                    info!(actor = ctx.name(), "Consumer stopping");
                    return Ok(());
                }

                // Process one tick
                let response = match self.consumer.tick().await {
                    Ok(status) => {
                        self.breaker.record_success();
                        self.next_action(status)
                    }
                    Err(e) => self.handle_tick_error(e).await?,
                };

                // Schedule the next tick based on the response
                match response {
                    TickResponse::ScheduleImmediate => {
                        // Schedule another tick immediately
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
                        // Schedule tick after delay
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
                        // No more ticks needed
                        info!("Consumer finished");
                        self.running = false;
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

            ConsumerMsg::Stop => {
                info!(actor = ctx.name(), "Stopping consumer");
                self.running = false;

                if let Err(e) = self.consumer.stop().await {
                    error!(error = %e, "Consumer stop failed");
                    return Err(ActorError::Internal(e.to_string()));
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
