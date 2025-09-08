use crate::{
    buffer::SledBuffer,
    context::item::ItemContext,
    producer::DataProducer,
    source::Source,
    transform::{
        computed::ComputedTransform,
        mapping::{FieldMapper, TableMapper},
        pipeline::{TransformPipeline, TransformPipelineExt},
    },
};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::{watch::Sender, Mutex};
use tracing::{error, info};

pub struct LiveProducer {
    buffer: Arc<SledBuffer>,
    source: Source,
    pipeline: TransformPipeline,
    shutdown_sender: Sender<bool>,
    batch_size: usize,
}

impl LiveProducer {
    pub async fn new(ctx: &ItemContext, sender: Sender<bool>) -> Self {
        let buffer = Arc::clone(&ctx.buffer);
        let source = ctx.source.clone();

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

        let batch_size = ctx.state.lock().await.batch_size();

        Self {
            buffer,
            source,
            batch_size,
            shutdown_sender: sender,
            pipeline,
        }
    }
}

#[async_trait]
impl DataProducer for LiveProducer {
    async fn run(&mut self) -> usize {
        let mut offset = 0; //self.buffer.read_last_offset();
        let mut batch_number = 1;

        loop {
            info!(
                "Fetching batch #{batch_number} with offset {offset} and batch size {0}",
                self.batch_size
            );

            match self.source.fetch_data(self.batch_size, Some(offset)).await {
                Ok(records) if records.is_empty() => {
                    info!("No more records to fetch. Terminating producer.");
                    break;
                }
                Ok(records) => {
                    info!("Fetched {} records in batch #{batch_number}", records.len());

                    for record in records.iter() {
                        // Apply the transformation pipeline to each record
                        let transformed_record = self.pipeline.apply(record);

                        // Store the transformed record in the buffer
                        if let Err(e) = self.buffer.store(transformed_record.serialize()) {
                            error!("Failed to store record: {}", e);
                            return batch_number;
                        }
                    }

                    offset += self.batch_size;
                    if let Err(e) = self.buffer.store_last_offset(offset) {
                        error!(
                            "Failed to persist last offset after batch #{batch_number}: {}",
                            e
                        );
                        break;
                    }
                }
                Err(e) => {
                    error!(
                        "Failed to fetch batch #{batch_number} at offset {}: {}",
                        offset, e
                    );
                    break;
                }
            }

            batch_number += 1;
        }

        // Notify the consumer to shutdown
        self.shutdown_sender.send(true).unwrap();

        batch_number
    }
}
