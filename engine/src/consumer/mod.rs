use crate::{
    consumer::{live::LiveConsumer, validation::ValidationConsumer},
    context::item::ItemContext,
    error::ConsumerError,
};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::{watch::Receiver, Mutex};

pub mod live;
pub mod trigger;
pub mod validation;

#[async_trait]
pub trait DataConsumer {
    /// Executes the consumer's main loop.
    async fn run(&mut self) -> Result<(), ConsumerError>;
}

pub async fn create_consumer(
    ctx: &Arc<Mutex<ItemContext>>,
    receiver: Receiver<bool>,
) -> Box<dyn DataConsumer + Send> {
    let ctx_guard = ctx.lock().await;
    let state_guard = ctx_guard.state.lock().await;

    if state_guard.is_dry_run() {
        Box::new(ValidationConsumer::new())
    } else {
        let buffer = Arc::clone(&ctx_guard.buffer);
        let destination = ctx_guard.destination.clone();
        let mappings = ctx_guard.mapping.clone();
        let batch_size = state_guard.batch_size();

        // Drop guards to release locks before creating the new object.
        drop(state_guard);
        drop(ctx_guard);

        Box::new(LiveConsumer::new(
            buffer,
            destination,
            mappings,
            receiver,
            batch_size,
        ))
    }
}
