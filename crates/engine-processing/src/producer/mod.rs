use crate::{
    error::ProducerError,
    producer::{live::LiveProducer, validation::ValidationProducer},
    transform::{
        computed::ComputedTransform,
        mapping::{FieldMapper, TableMapper},
        pipeline::{TransformPipeline, TransformPipelineExt},
    },
};
use async_trait::async_trait;
use engine_config::report::dry_run::DryRunReport;
use engine_core::context::item::ItemContext;
use futures::lock::Mutex;
use model::{records::batch::Batch, transform::mapping::EntityMapping};
use smql_syntax::ast::setting::Settings;
use std::sync::Arc;
use tokio::sync::{mpsc, watch::Sender};
use tokio_util::sync::CancellationToken;

pub mod live;
pub mod validation;

fn pipeline_for_mapping(mapping: &EntityMapping) -> TransformPipeline {
    let mut pipeline = TransformPipeline::new();

    pipeline = pipeline
        .add_if(!mapping.entity_name_map.is_empty(), || {
            TableMapper::new(mapping.entity_name_map.clone())
        })
        .add_if(!mapping.field_mappings.is_empty(), || {
            FieldMapper::new(mapping.field_mappings.clone())
        })
        .add_if(!mapping.field_mappings.is_empty(), || {
            ComputedTransform::new(mapping.clone())
        });

    pipeline
}

#[async_trait]
pub trait DataProducer {
    /// Executes the producer's main loop.
    async fn run(&mut self) -> Result<usize, ProducerError>;
}

pub async fn create_producer(
    ctx: &Arc<Mutex<ItemContext>>,
    shutdown_tx: Sender<bool>,
    batch_tx: mpsc::Sender<Batch>,
    settings: &Settings,
    cancel: CancellationToken,
    report: &Arc<Mutex<Option<DryRunReport>>>,
) -> Box<dyn DataProducer + Send> {
    let (is_dry_run, source, destination, mapping, offset_strategy, cursor) = {
        let guard = ctx.lock().await;
        let is_dry_run = guard.settings.lock().await.is_dry_run();
        (
            is_dry_run,
            guard.source.clone(),
            guard.destination.clone(),
            guard.mapping.clone(),
            guard.offset_strategy.clone(),
            guard.cursor.clone(),
        )
    };

    if is_dry_run {
        let has_report = report.lock().await.is_some();
        if has_report {
            let pipeline = pipeline_for_mapping(&mapping);
            let validation_prod = ValidationProducer::new(
                source,
                destination,
                pipeline,
                mapping,
                settings.clone(),
                offset_strategy,
                cursor,
                report.clone(),
            );
            return Box::new(validation_prod);
        }
    }

    let live_prod = LiveProducer::new(ctx, shutdown_tx, batch_tx, settings, cancel).await;
    Box::new(live_prod)
}
