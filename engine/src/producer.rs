use crate::{context::MigrationContext, source::data_source::DataSource};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{error, info};

pub async fn spawn_producer(context: Arc<Mutex<MigrationContext>>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut offset = 0;
        let (buffer, data_source, batch_size) = {
            let context_guard = context.lock().await;
            let buffer = Arc::clone(&context_guard.buffer);
            let data_source = match &context_guard.source {
                DataSource::Database(db) => Arc::clone(db),
            };
            let batch_size = context_guard.state.lock().await.batch_size;

            (buffer, data_source, batch_size)
        };

        loop {
            match data_source.fetch_data(batch_size, Some(offset)).await {
                Ok(records) if records.is_empty() => break,
                Ok(records) => {
                    info!("Fetched {} records", records.len());
                    for record in records {
                        if let Err(e) = buffer.store(record.serialize()) {
                            info!("Failed to store record: {}", e);
                            return;
                        }
                    }
                }
                Err(e) => {
                    error!("Error fetching data: {}", e);
                    break;
                }
            }

            offset += batch_size;
        }
    })
}
