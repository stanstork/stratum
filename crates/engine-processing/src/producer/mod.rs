use crate::{
    error::ProducerError,
    producer::{
        live::LiveProducer,
        validation::{ValidationProducer, ValidationProducerParams},
    },
    transform::{
        computed::ComputedTransform,
        mapping::{FieldMapper, TableMapper},
        pipeline::{TransformPipeline, TransformPipelineExt},
    },
};
use async_trait::async_trait;
use engine_config::{report::dry_run::DryRunReport, settings::validated::ValidatedSettings};
use engine_core::context::item::ItemContext;
use futures::lock::Mutex;
use model::{records::batch::Batch, transform::mapping::EntityMapping};
use std::sync::Arc;
use tokio::sync::mpsc;

pub mod components;
pub mod config;
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

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ProducerStatus {
    /// Work is ongoing; the actor should schedule another tick.
    Working,
    /// The producer is idle (e.g. waiting for CDC events or backpressure).
    Idle,
    /// The producer has finished its task (e.g. Snapshot complete).
    Finished,
}

#[async_trait]
pub trait DataProducer {
    async fn start_snapshot(&mut self) -> Result<(), ProducerError>;
    async fn start_cdc(&mut self) -> Result<(), ProducerError>;

    async fn resume(
        &mut self,
        run_id: &str,
        item_id: &str,
        part_id: &str,
    ) -> Result<(), ProducerError>;

    async fn tick(&mut self) -> Result<ProducerStatus, ProducerError>;
    async fn stop(&mut self) -> Result<(), ProducerError>;

    fn rows_produced(&self) -> u64;
}

pub async fn create_producer(
    ctx: &Arc<Mutex<ItemContext>>,
    batch_tx: mpsc::Sender<Batch>,
    settings: &ValidatedSettings,
    report: &Arc<Mutex<DryRunReport>>,
) -> Box<dyn DataProducer + Send + 'static> {
    let (source, destination, mapping, offset_strategy, cursor) = {
        let guard = ctx.lock().await;
        (
            guard.source.clone(),
            guard.destination.clone(),
            guard.mapping.clone(),
            guard.offset_strategy.clone(),
            guard.cursor.clone(),
        )
    };

    if settings.is_dry_run() {
        let pipeline = pipeline_for_mapping(&mapping);
        let validation_prod = ValidationProducer::new(ValidationProducerParams {
            source,
            destination,
            pipeline,
            mapping,
            settings: settings.clone(),
            offset_strategy,
            cursor,
            report: report.clone(),
        });
        return Box::new(validation_prod);
    }

    let live_prod = LiveProducer::new(ctx, batch_tx, settings).await;
    Box::new(live_prod)
}
