use crate::{
    buffer::SledBuffer,
    context::MigrationContext,
    destination::data_dest::{DataDestination, DbDataDestination},
    record::{DataRecord, Record},
};
use sql_adapter::{metadata::table::TableMetadata, row::row_data::RowData};
use std::{collections::HashMap, sync::Arc, time::Instant};
use tokio::sync::{watch, Mutex};
use tracing::{error, info};

pub struct Consumer {
    buffer: Arc<SledBuffer>,
    data_destination: Arc<Mutex<dyn DbDataDestination>>,
    tbl_names_map: HashMap<String, String>,
    batch_size: usize,
    shutdown_receiver: watch::Receiver<bool>,
}

impl Consumer {
    pub async fn new(
        context: Arc<Mutex<MigrationContext>>,
        receiver: watch::Receiver<bool>,
    ) -> Self {
        let context_guard = context.lock().await;
        let buffer = Arc::clone(&context_guard.buffer);
        let data_destination = match &context_guard.destination {
            DataDestination::Database(db) => Arc::clone(db),
        };
        let tbl_names_map = context_guard.src_dst_name_map.clone();
        let batch_size = context_guard.state.lock().await.batch_size;

        Self {
            buffer,
            data_destination,
            tbl_names_map,
            batch_size,
            shutdown_receiver: receiver,
        }
    }

    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move { self.run().await })
    }

    async fn run(self) {
        let tables = self.data_destination.lock().await.metadata().tables();

        info!("Disabling triggers for all tables");
        self.toggle_trigger(&tables, false).await;

        let mut batch_map = HashMap::new();

        loop {
            match self.buffer.read_next() {
                Some(record) => {
                    self.process_record(record, &mut batch_map, &tables).await;
                }
                None => {
                    self.flush_all(&mut batch_map, &tables).await;

                    // If the shutdown signal is received, it means the producer has finished
                    // processing all records and the consumer can safely exit if the buffer is empty
                    if *self.shutdown_receiver.borrow() {
                        break;
                    }

                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
        }

        info!("Enabling triggers for all tables");
        self.toggle_trigger(&tables, true).await;

        info!("Consumer finished");
    }

    async fn process_record(
        &self,
        record: Vec<u8>,
        batch_map: &mut HashMap<String, Vec<Record>>,
        tables: &Vec<TableMetadata>,
    ) {
        let row_data = RowData::deserialize(record);
        let table_name = self
            .tbl_names_map
            .get(&row_data.table_name)
            .unwrap_or(&row_data.table_name);

        let batch = batch_map.entry(table_name.clone()).or_default();
        batch.push(Record::RowData(row_data));

        if batch.len() >= self.batch_size {
            self.flush_all(batch_map, tables).await;
        }
    }

    async fn flush_all(
        &self,
        batch_map: &mut HashMap<String, Vec<Record>>,
        tables: &Vec<TableMetadata>,
    ) {
        for table in tables.iter() {
            // Get the table name from the map or use the original name if no mapping is found
            // This is needed when the source and destination table names are different
            let table_name = self.tbl_names_map.get(&table.name).unwrap_or(&table.name);

            if let Some(records) = batch_map.remove(table_name) {
                if records.is_empty() {
                    // Skip empty batch
                    return;
                }

                let start_time = Instant::now();

                match self
                    .data_destination
                    .lock()
                    .await
                    .write_batch(table, records)
                    .await
                {
                    Ok(_) => {
                        let elapsed = start_time.elapsed().as_millis();
                        info!(
                            "Batch for table {} written successfully in {}ms",
                            table.name, elapsed
                        );
                    }
                    Err(e) => panic!("Failed to write batch: {}", e),
                }
            }
        }
    }

    async fn toggle_trigger(&self, tables: &Vec<TableMetadata>, enable: bool) {
        for table in tables.iter() {
            if let Err(e) = self
                .data_destination
                .lock()
                .await
                .toggle_trigger(&table.name, enable)
                .await
            {
                error!("Failed to toggle trigger for table {}: {}", table.name, e);
            }
        }
    }
}
