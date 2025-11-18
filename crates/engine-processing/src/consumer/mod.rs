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
    let ctx_guard = ctx.lock().await;
    let settings_guard = ctx_guard.settings.lock().await;
    let is_dry_run = settings_guard.is_dry_run();

    drop(settings_guard);
    drop(ctx_guard);

    if is_dry_run {
        Box::new(ValidationConsumer::new())
    } else {
        Box::new(LiveConsumer::new(ctx, batch_rx, shutdown_rx, cancel).await)
    }
}
