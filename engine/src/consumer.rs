use crate::{
    buffer::SledBuffer,
    context::item::ItemContext,
    destination::{data_dest::DataDestination, destination::Destination},
    record::{DataRecord, Record},
};
use common::mapping::EntityMapping;
use sql_adapter::{metadata::table::TableMetadata, row::row_data::RowData};
use std::{collections::HashMap, sync::Arc, time::Instant};
use tokio::sync::{watch::Receiver, Mutex};
use tracing::{error, info};

pub struct Consumer {
    buffer: Arc<SledBuffer>,
    destination: Destination,
    mappings: EntityMapping,
    shutdown_receiver: Receiver<bool>,
    batch_size: usize,
}

impl Consumer {
    pub async fn new(ctx: Arc<Mutex<ItemContext>>, receiver: Receiver<bool>) -> Self {
        let ctx = ctx.lock().await;
        let buffer = Arc::clone(&ctx.buffer);
        let destination = ctx.destination.clone();
        let mappings = ctx.mapping.clone();
        let batch_size = ctx.state.lock().await.batch_size;

        Self {
            buffer,
            destination,
            mappings,
            shutdown_receiver: receiver,
            batch_size,
        }
    }

    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move { self.run().await })
    }

    async fn run(self) {
        let tables = match &self.destination.data_dest {
            DataDestination::Database(db) => db.lock().await.get_tables(),
        };

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
        let table_name = row_data.table.clone(); //self.table_name_map.resolve(&row_data.table);

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
            let table_name = self.mappings.entity_name_map.resolve(&table.name);

            if let Some(records) = batch_map.remove(&table_name) {
                if records.is_empty() {
                    // Skip empty batch
                    return;
                }

                let start_time = Instant::now();

                match self.destination.write_batch(table, records).await {
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

    async fn toggle_trigger(&self, tables: &[TableMetadata], enable: bool) {
        for table in tables.iter() {
            if let Err(e) = self.destination.toggle_trigger(&table.name, enable).await {
                error!("Failed to toggle trigger for table {}: {}", table.name, e);
            }
        }
    }
}
