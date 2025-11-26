use async_trait::async_trait;
use engine_core::metrics::Metrics;
use engine_processing::{
    cb::{CircuitBreaker, CircuitBreakerState},
    producer::{DataProducer, ProducerStatus},
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::{
    actor::{
        actor::{Actor, ActorContext},
        messages::ProducerMsg,
    },
    error::ActorError,
};

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
            }

            ProducerMsg::Tick => {
                if !self.running || self.cancel_token.is_cancelled() {
                    return Ok(());
                }

                match self.producer.tick().await {
                    Ok(ProducerStatus::Working) => {
                        self.breaker.record_success();
                    }
                    Ok(ProducerStatus::Idle) => {
                        self.breaker.record_success();
                        // Idle, but check again after a short delay to keep the loop alive
                        // or wait for external triggers. For now, we poll slowly.
                        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    }
                    Ok(ProducerStatus::Finished) => {
                        info!(actor = ctx.name(), "Producer reported finished state.");
                        self.running = false;
                        let _ = self.producer.stop().await;
                        // Do not schedule next tick.
                    }
                    Err(e) => {
                        self.metrics.increment_failures(1);
                        error!(actor = ctx.name(), error = %e, "Producer tick failed");

                        match self.breaker.record_failure() {
                            CircuitBreakerState::RetryAfter(delay) => {
                                warn!("Circuit breaker: backing off for {:?}", delay);
                                tokio::time::sleep(delay).await;
                            }
                            CircuitBreakerState::Open => {
                                error!("Circuit breaker open. Stopping producer.");
                                self.running = false;
                            }
                        }
                    }
                }
            }

            ProducerMsg::Stop => {
                info!(actor = ctx.name(), "Stopping producer");
                self.running = false;
                let _ = self.producer.stop().await;
            }
        }
        Ok(())
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
