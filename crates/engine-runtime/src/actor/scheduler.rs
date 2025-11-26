use crate::{
    actor::{actor::ActorRef, messages::ProducerMsg},
    error::ActorError,
};
use std::time::Duration;
use tokio::time;

pub struct TickScheduler {
    actor_ref: ActorRef<ProducerMsg>,
    immediate_delay: Duration,
    idle_delay: Duration,
}

impl TickScheduler {
    pub fn new(
        actor_ref: ActorRef<ProducerMsg>,
        immediate_delay: Duration,
        idle_delay: Duration,
    ) -> Self {
        Self {
            actor_ref,
            immediate_delay,
            idle_delay,
        }
    }

    pub async fn schedule_immediate(&self) -> Result<(), ActorError> {
        time::sleep(self.immediate_delay).await;
        self.actor_ref.send(ProducerMsg::Tick).await
    }

    pub async fn schedule_idle(&self) -> Result<(), ActorError> {
        time::sleep(self.idle_delay).await;
        self.actor_ref.send(ProducerMsg::Tick).await
    }
}
