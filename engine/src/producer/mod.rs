use crate::{
    context::item::ItemContext,
    producer::{live::LiveProducer, validation::ValidationProducer},
    transform::{
        computed::ComputedTransform,
        mapping::{FieldMapper, TableMapper},
        pipeline::{TransformPipeline, TransformPipelineExt},
    },
};
use async_trait::async_trait;
use smql::statements::setting::CopyColumns;
use std::sync::Arc;
use tokio::sync::{watch::Sender, Mutex};

pub mod live;
pub mod schema_validator;
pub mod validation;

#[async_trait]
pub trait DataProducer {
    /// Executes the producer's main loop.
    async fn run(&mut self) -> usize;
}

pub async fn create_producer(
    ctx: &Arc<Mutex<ItemContext>>,
    sender: Sender<bool>,
) -> Box<dyn DataProducer + Send> {
    let is_validation_run = {
        let ctx_guard = ctx.lock().await;
        let state = ctx_guard.state.lock().await;
        state.is_dry_run
    };

    if is_validation_run {
        let (state, source, destination, pipeline) = {
            let ctx_guard = ctx.lock().await;
            (
                ctx_guard.state.clone(),
                ctx_guard.source.clone(),
                ctx_guard.destination.clone(),
                create_pipeline_from_context(&ctx_guard),
            )
        };
        let mapping = {
            let ctx_guard = ctx.lock().await;
            ctx_guard.mapping.clone()
        };
        let settings = {
            let state_guard = state.lock().await;
            state_guard.settings.clone()
        };

        Box::new(ValidationProducer::new(
            state,
            source,
            destination,
            pipeline,
            mapping,
            settings,
        ))
    } else {
        let (buffer, source, pipeline) = {
            let ctx_guard = ctx.lock().await;
            let mut pipeline = TransformPipeline::new();
            let mapping = ctx_guard.mapping.clone();

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
            (
                Arc::clone(&ctx_guard.buffer),
                ctx_guard.source.clone(),
                pipeline,
            )
        };

        let batch_size = {
            let state_arc = {
                let ctx_guard = ctx.lock().await;
                ctx_guard.state.clone()
            };
            let state = state_arc.lock().await;
            state.batch_size()
        };

        Box::new(LiveProducer::new(
            buffer, source, pipeline, sender, batch_size,
        ))
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
