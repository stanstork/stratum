use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::{watch::Sender, Mutex};

use crate::{
    context::item::ItemContext,
    producer::{live::LiveProducer, validation::ValidationProducer},
    transform::{
        computed::ComputedTransform,
        mapping::{FieldMapper, TableMapper},
        pipeline::{TransformPipeline, TransformPipelineExt},
    },
};

pub mod live;
pub mod validation;

#[async_trait]
pub trait DataProducer {
    /// Executes the producer's main loop.
    /// This method consumes the producer as it's meant to be run once.
    async fn run(&mut self) -> usize;
}

pub async fn create_producer(
    ctx: &Arc<Mutex<ItemContext>>,
    sender: Sender<bool>,
) -> Box<dyn DataProducer + Send> {
    let context_guard = ctx.lock().await;
    let state_guard = context_guard.state.lock().await;

    if state_guard.is_validation_run {
        // Create and return the ValidationProducer
        Box::new(ValidationProducer::new(&context_guard, sender).await)
    } else {
        // Create and return the LiveProducer
        Box::new(LiveProducer::new(&context_guard, sender).await)
    }
}

fn create_pipeline_from_context(ctx: &ItemContext) -> TransformPipeline {
    let mut pipeline = TransformPipeline::new();

    pipeline = pipeline
        .add_if(!ctx.mapping.entity_name_map.is_empty(), || {
            TableMapper::new(ctx.mapping.entity_name_map.clone())
        })
        .add_if(!ctx.mapping.field_mappings.is_empty(), || {
            FieldMapper::new(ctx.mapping.field_mappings.clone())
        })
        .add_if(!ctx.mapping.field_mappings.is_empty(), || {
            ComputedTransform::new(ctx.mapping.clone())
        });

    pipeline
}
