use crate::{
    buffer::SledBuffer,
    context::MigrationContext,
    source::data_source::{DataSource, DbDataSource},
};
use std::sync::Arc;
use tokio::sync::{watch, Mutex};
use tracing::{error, info};

pub struct Producer {
    buffer: Arc<SledBuffer>,
    data_source: Arc<Mutex<dyn DbDataSource>>,
    batch_size: usize,
    shutdown_sender: watch::Sender<bool>,
}

impl Producer {
    pub async fn new(context: Arc<Mutex<MigrationContext>>, sender: watch::Sender<bool>) -> Self {
        let context_guard = context.lock().await;
        let buffer = Arc::clone(&context_guard.buffer);
        let data_source = match &context_guard.source {
            DataSource::Database(db) => Arc::clone(db),
        };
        let batch_size = context_guard.state.lock().await.batch_size;

        Self {
            buffer,
            data_source,
            batch_size,
            shutdown_sender: sender,
        }
    }

    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let batch_number = self.run().await;
            info!(
                "Producer finished after processing {} batches",
                batch_number - 1
            );
        })
    }

    async fn run(self) -> usize {
        let mut offset = 0; //self.buffer.read_last_offset();
        let mut batch_number = 1;

        loop {
            info!(
                "Fetching batch #{batch_number} with offset {offset} and batch size {0}",
                self.batch_size
            );

            match self
                .data_source
                .lock()
                .await
                .fetch_data(self.batch_size, Some(offset))
                .await
            {
                Ok(records) if records.is_empty() => {
                    info!("No more records to fetch. Terminating producer.");
                    break;
                }
                Ok(records) => {
                    info!("Fetched {} records in batch #{batch_number}", records.len());

                    for record in records {
                        if let Err(e) = self.buffer.store(record.serialize()) {
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
