use crate::{
    consumer::{live::LiveConsumer, validation::ValidationConsumer},
    context::item::ItemContext,
};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::{watch::Receiver, Mutex};

pub mod live;
pub mod validation;

#[async_trait]
pub trait DataConsumer {
    /// Executes the consumer's main loop.
    async fn run(&self);
}

pub async fn create_consumer(
    ctx: &Arc<Mutex<ItemContext>>,
    receiver: Receiver<bool>,
) -> Box<dyn DataConsumer + Send> {
    let is_validation_run = {
        let ctx_guard = ctx.lock().await;
        let state = ctx_guard.state.lock().await;
        state.is_dry_run
    };

    if is_validation_run {
        Box::new(ValidationConsumer::new())
    } else {
        let (buffer, destination, mappings, batch_size) = {
            let ctx_guard = ctx.lock().await;
            let mappings = ctx_guard.mapping.clone();
            let batch_size = ctx_guard.state.lock().await.batch_size();

            (
                Arc::clone(&ctx_guard.buffer),
                ctx_guard.destination.clone(),
                mappings,
                batch_size,
            )
        };

        Box::new(LiveConsumer::new(
            buffer,
            destination,
            mappings,
            receiver,
            batch_size,
        ))
    }
}
