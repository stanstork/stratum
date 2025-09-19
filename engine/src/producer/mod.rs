use crate::{
    context::item::ItemContext,
    error::ProducerError,
    producer::{live::LiveProducer, validation::ValidationProducer},
    transform::{
        computed::ComputedTransform,
        mapping::{FieldMapper, TableMapper},
        pipeline::{TransformPipeline, TransformPipelineExt},
    },
};
use async_trait::async_trait;
use common::mapping::EntityMapping;
use smql::statements::setting::Settings;
use std::sync::Arc;
use tokio::sync::{watch::Sender, Mutex};

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
    settings: &Settings,
) -> Box<dyn DataProducer + Send> {
    let (state, source, destination, buffer, mapping) = {
        let c = ctx.lock().await;
        (
            c.state.clone(),
            c.source.clone(),
            c.destination.clone(),
            Arc::clone(&c.buffer),
            c.mapping.clone(),
        )
    };

    let (is_dry_run, batch_size) = {
        let st = state.lock().await;
        (st.is_dry_run(), st.batch_size())
    };

    let pipeline = pipeline_for_mapping(&mapping);

    if is_dry_run {
        Box::new(ValidationProducer::new(
            state,
            source,
            destination,
            pipeline,
            mapping,
            settings.clone(),
        ))
    } else {
        Box::new(LiveProducer::new(
            buffer,
            source,
            pipeline,
            shutdown_tx,
            batch_size,
        ))
    }
}
