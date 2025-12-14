use crate::{
    consumer::{live::LiveConsumer, validation::ValidationConsumer},
    error::ConsumerError,
};
use async_trait::async_trait;
use engine_config::settings::validated::ValidatedSettings;
use engine_core::{context::item::ItemContext, metrics::Metrics};
use futures::lock::Mutex;
use model::records::batch::Batch;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub mod components;
pub mod config;
pub mod live;
pub mod trigger;
pub mod validation;

#[derive(Clone, Debug, PartialEq)]
pub enum ConsumerStatus {
    /// Work is ongoing; the actor should schedule another tick immediately.
    Working,
    /// The consumer is idle (waiting for batches).
    Idle,
    /// The consumer has finished (channel closed, all work done).
    Finished,
}

#[async_trait]
pub trait DataConsumer {
    async fn start(&mut self) -> Result<(), ConsumerError>;

    async fn resume(
        &mut self,
        run_id: &str,
        item_id: &str,
        part_id: &str,
    ) -> Result<(), ConsumerError>;

    async fn tick(&mut self) -> Result<ConsumerStatus, ConsumerError>;
    async fn stop(&mut self) -> Result<(), ConsumerError>;

    /// Returns the total number of rows written to the destination.
    fn rows_written(&self) -> u64;
}

pub async fn create_consumer(
    item_ctx: &Arc<Mutex<ItemContext>>,
    batch_rx: mpsc::Receiver<Batch>,
    settings: &ValidatedSettings,
    cancel: CancellationToken,
    metrics: Metrics,
) -> Box<dyn DataConsumer + Send + 'static> {
    if settings.is_dry_run() {
        Box::new(ValidationConsumer::new(batch_rx))
    } else {
        Box::new(LiveConsumer::new(item_ctx, batch_rx, cancel, metrics).await)
    }
}
