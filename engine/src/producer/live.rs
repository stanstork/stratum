use crate::{
    buffer::SledBuffer, error::ProducerError, producer::DataProducer, source::Source,
    transform::pipeline::TransformPipeline,
};
use async_trait::async_trait;
use futures::future::join_all;
use std::sync::Arc;
use tokio::sync::watch::Sender;
use tracing::{error, info};

pub struct LiveProducer {
    buffer: Arc<SledBuffer>,
    source: Source,
    pipeline: TransformPipeline,
    shutdown_tx: Sender<bool>,
    batch_size: usize,
}

impl LiveProducer {
    pub fn new(
        buffer: Arc<SledBuffer>,
        source: Source,
        pipeline: TransformPipeline,
        shutdown_tx: Sender<bool>,
        batch_size: usize,
    ) -> Self {
        Self {
            buffer,
            source,
            pipeline,
            shutdown_tx,
            batch_size,
        }
    }
}

#[async_trait]
impl DataProducer for LiveProducer {
    async fn run(&mut self) -> Result<usize, ProducerError> {
        let mut offset = 0; //self.buffer.read_last_offset();
        let mut batch_no: usize = 1;

        loop {
            info!(batch_no, batch_size = self.batch_size, "Fetching batch.");
            match self.source.fetch_data(self.batch_size, None).await {
                Ok(records) if records.is_empty() => {
                    info!("No more records to fetch. Terminating producer.");
                    break;
                }
                Ok(records) => {
                    info!(count = records.len(), "Fetched records in batch.");

                    // Transform records concurrently
                    let transform_futures = records.iter().map(|record| {
                        let pipeline = &self.pipeline;
                        async move { pipeline.apply(record) }
                    });
                    let transformed_records = join_all(transform_futures).await;

                    // Store transformed records in the buffer
                    for record in transformed_records {
                        self.buffer
                            .store(record.serialize())
                            .map_err(|e| ProducerError::Store(e.to_string()))?;
                    }

                    offset += self.batch_size;
                    self.buffer
                        .store_last_offset(offset)
                        .map_err(|e| ProducerError::StoreOffset(e.to_string()))?;
                }
                Err(e) => {
                    error!("Failed to fetch batch #{batch_no} at offset {offset}: {e}");
                    return Err(ProducerError::Fetch {
                        source: Box::new(e),
                    });
                }
            }

            batch_no += 1;
        }

        // Try to notify consumer; do not crash if the receiver is gone.
        let _ = self.shutdown_tx.send(true);

        Ok(batch_no - 1) // Return the number of batches processed
    }
}
