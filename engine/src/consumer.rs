use crate::{
    context::MigrationContext, destination::data_dest::DataDestination,
    record::deserialize_data_record,
};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

pub async fn spawn_consumer(context: Arc<Mutex<MigrationContext>>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let (buffer, _) = {
            let context_guard = context.lock().await;
            let buffer = Arc::clone(&context_guard.buffer);
            let data_destination = match &context_guard.destination {
                DataDestination::Database(db) => Arc::clone(db),
            };

            (buffer, data_destination)
        };

        loop {
            // Retrieve the next record
            if let Some(record) = buffer.read_next() {
                info!("Consuming record");
                let row_data = deserialize_data_record("RowData", record).await;
                row_data.debug();
            } else {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                // Avoid busy looping
            }
        }
    })
}
