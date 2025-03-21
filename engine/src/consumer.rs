use crate::{
    context::MigrationContext, destination::data_dest::DataDestination, record::DataRecord,
};
use sql_adapter::{metadata::provider::MetadataProvider, row::row_data::RowData};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

pub async fn spawn_consumer(context: Arc<Mutex<MigrationContext>>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let (buffer, data_destination, src_dst_name_map) = {
            let context_guard = context.lock().await;
            let buffer = Arc::clone(&context_guard.buffer);
            let data_destination = match &context_guard.destination {
                DataDestination::Database(db) => Arc::clone(db),
            };
            let src_dst_name_map = context_guard.src_dst_name_map.clone();

            (buffer, data_destination, src_dst_name_map)
        };

        let metadata = data_destination.lock().await.metadata().clone();
        let ordered_meta = MetadataProvider::resolve_insert_order(&metadata);

        loop {
            // Retrieve the next record
            if let Some(record) = buffer.read_next() {
                info!("Consuming record");

                let row_data = RowData::deserialize(record);
                for table in ordered_meta.iter() {
                    let table_name = src_dst_name_map.get(&table.name).unwrap_or(&table.name);
                    let columns = row_data.extract_table_columns(&table_name);

                    println!("Columns: {:#?}", columns);
                }
            } else {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                // Avoid busy looping
            }
        }
    })
}
