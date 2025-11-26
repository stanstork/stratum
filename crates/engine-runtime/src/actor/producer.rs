use crate::{
    actor::{
        actor::{Actor, ActorContext, ActorRef},
        messages::ProducerMsg,
    },
    error::ActorError,
};
use async_trait::async_trait;
use engine_core::metrics::Metrics;
use engine_processing::{
    cb::{CircuitBreaker, CircuitBreakerState},
    producer::{DataProducer, ProducerStatus},
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

pub struct ProducerActor<P>
where
    P: DataProducer + Send + 'static,
{
    producer: P,

    // Control state
    running: bool,
    cancel_token: CancellationToken,

    // Resilience
    breaker: CircuitBreaker,
    metrics: Metrics,

    // Configuration
    idle_delay: Duration,

    // Self-reference for scheduling ticks
    actor_ref: Option<ActorRef<ProducerMsg>>,
}

impl<P> ProducerActor<P>
where
    P: DataProducer + Send + 'static,
{
    pub fn new(producer: P, cancel_token: CancellationToken, metrics: Metrics) -> Self {
        Self {
            producer,
            running: false,
            cancel_token,
            breaker: CircuitBreaker::default_db(),
            metrics,
            idle_delay: Duration::from_millis(500), // Default idle delay
            actor_ref: None,
        }
    }

    /// This must be called after the actor is spawned.
    pub fn set_actor_ref(&mut self, actor_ref: ActorRef<ProducerMsg>) {
        self.actor_ref = Some(actor_ref);
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
impl<P> Actor<ProducerMsg> for ProducerActor<P>
where
    P: DataProducer + Send + 'static,
{
    async fn on_start(&mut self, ctx: &ActorContext) -> Result<(), ActorError> {
        info!(actor = ctx.name(), "Producer actor started");
        Ok(())
    }

    async fn handle(&mut self, msg: ProducerMsg, ctx: &ActorContext) -> Result<(), ActorError> {
        match msg {
            ProducerMsg::SetActorRef(actor_ref) => {
                info!(
                    actor = ctx.name(),
                    "Setting actor reference for tick scheduling"
                );
                self.set_actor_ref(actor_ref);
                Ok(())
            }

            ProducerMsg::StartSnapshot { run_id, item_id } => {
                info!(actor = ctx.name(), run_id = %run_id, item_id = %item_id, "Starting snapshot");

                if let Err(e) = self.producer.resume(&run_id, &item_id, "part-0").await {
                    error!("Failed to resume producer state: {}", e);
                    return Err(ActorError::Internal(e.to_string()));
                }

                if let Err(e) = self.producer.start_snapshot().await {
                    error!("Failed to start snapshot: {}", e);
                    return Err(ActorError::Internal(e.to_string()));
                }

                self.running = true;

                // Send the first tick to start the processing loop
                if let Some(ref actor_ref) = self.actor_ref {
                    actor_ref.try_send(ProducerMsg::Tick)?;
                }

                Ok(())
            }

            ProducerMsg::StartCdc {
                run_id: _,
                item_id: _,
            } => {
                info!(actor = ctx.name(), "Starting CDC");
                if let Err(e) = self.producer.start_cdc().await {
                    error!("Failed to start CDC: {}", e);
                    return Err(ActorError::Internal(e.to_string()));
                }
                self.running = true;

                // Send the first tick to start the processing loop
                if let Some(ref actor_ref) = self.actor_ref {
                    actor_ref.try_send(ProducerMsg::Tick)?;
                }

                Ok(())
            }

            ProducerMsg::Tick => {
                // Stop if not running or cancelled
                if !self.running || self.cancel_token.is_cancelled() {
                    info!(actor = ctx.name(), "Producer stopping");
                    return Ok(());
                }

                let response = match self.producer.tick().await {
                    Ok(status) => {
                        self.breaker.record_success();
                        self.next_action(status)
                    }
                    Err(e) => self.handle_tick_error(e).await?,
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
                        info!("Producer finished");
                        self.running = false;
                        let _ = self.producer.stop().await;
                    }
                }

                Ok(())
            }

            ProducerMsg::Stop => {
                info!(actor = ctx.name(), "Stopping producer");
                self.running = false;
                if let Err(e) = self.producer.stop().await {
                    error!(error = %e, "Producer stop failed");
                    return Err(ActorError::Internal(e.to_string()));
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
