use crate::error::MigrationError;
use engine_config::report::dry_run::DryRunReport;
use engine_core::context::item::ItemContext;
use engine_processing::{consumer::create_consumer, producer::create_producer};
use futures::lock::Mutex;
use model::records::batch::Batch;
use smql_syntax::ast::setting::Settings;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

pub async fn spawn(
    ctx: Arc<Mutex<ItemContext>>,
    settings: &Settings,
    cancel: CancellationToken,
    dry_run_report: &Arc<Mutex<Option<DryRunReport>>>,
) -> Result<(), MigrationError> {
    info!("Launching workers");

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (tx, mut rx) = mpsc::channel::<Batch>(64);

    let mut producer =
        create_producer(&ctx, shutdown_tx, tx, settings, cancel, dry_run_report).await;
    let producer_handle = tokio::spawn(async move { producer.run().await });

    let mut consumer = create_consumer(&ctx, rx, shutdown_rx).await;
    let consumer_handle = tokio::spawn(async move { consumer.run().await });

    let (producer_result, consumer_result) = tokio::try_join!(producer_handle, consumer_handle)?;

    if let Err(err) = producer_result {
        error!("Producer error: {}", err);
        return Err(MigrationError::Unexpected(err.to_string()));
    }

    consumer_result.map_err(|err| {
        error!("Consumer error: {}", err);
        MigrationError::Unexpected(err.to_string())
    })
}
