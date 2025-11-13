use crate::{
    consumer::{live::LiveConsumer, validation::ValidationConsumer},
    error::ConsumerError,
};
use async_trait::async_trait;
use engine_core::context::item::ItemContext;
use futures::lock::Mutex;
use model::records::batch::Batch;
use std::sync::Arc;
use tokio::sync::{mpsc, watch::Receiver};
use tokio_util::sync::CancellationToken;

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
    batch_rx: mpsc::Receiver<Batch>,
    shutdown_rx: Receiver<bool>,
    cancel: CancellationToken,
) -> Box<dyn DataConsumer + Send> {
    Box::new(LiveConsumer::new(ctx, batch_rx, shutdown_rx, cancel).await)

    // Box::new(ValidationConsumer::new())
    // let ctx_guard = ctx.lock().await;
    // let state_guard = ctx_guard.state.lock().await;

    // if state_guard.is_dry_run() {
    //     Box::new(ValidationConsumer::new())
    // } else {
    //     let buffer = Arc::clone(&ctx_guard.buffer);
    //     let destination = ctx_guard.destination.clone();
    //     let mappings = ctx_guard.mapping.clone();
    //     let batch_size = state_guard.batch_size();

    //     // Drop guards to release locks before creating the new object.
    //     drop(state_guard);
    //     drop(ctx_guard);

    //     Box::new(LiveConsumer::new(
    //         buffer,
    //         destination,
    //         mappings,
    //         receiver,
    //         batch_size,
    //     ))
    // }
    // todo!("Implement consumer creation based on context and settings")
}
