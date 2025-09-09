use crate::{
    buffer::SledBuffer,
    consumer::DataConsumer,
    destination::{data::DataDestination, Destination},
    metrics::Metrics,
    report::metrics::{send_report, MetricsReport},
};
use async_trait::async_trait;
use common::{
    mapping::EntityMapping,
    record::{DataRecord, Record},
    row_data::RowData,
};
use sql_adapter::metadata::table::TableMetadata;
use std::{collections::HashMap, sync::Arc, time::Instant};
use tokio::sync::watch::Receiver;
use tracing::{error, info};

pub struct LiveConsumer {
    buffer: Arc<SledBuffer>,
    destination: Destination,
    mappings: EntityMapping,
    shutdown_receiver: Receiver<bool>,
    batch_size: usize,
}

#[async_trait]
impl DataConsumer for LiveConsumer {
    async fn run(&self) {
        let tables = match &self.destination.data_dest {
            DataDestination::Database(db) => db.lock().await.tables(),
        };

        info!("Disabling triggers for all tables");
        self.toggle_trigger(&tables, false).await;

        let mut batch_map = HashMap::new();
        let metrics = Metrics::new();

        loop {
            match self.buffer.read_next() {
                Some(record) => {
                    self.process_record(record, &mut batch_map, &tables, &metrics)
                        .await;
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

        self.send_final_report(&metrics).await;

        info!("Consumer finished");
    }
}

impl LiveConsumer {
    pub fn new(
        buffer: Arc<SledBuffer>,
        destination: Destination,
        mappings: EntityMapping,
        receiver: Receiver<bool>,
        batch_size: usize,
    ) -> Self {
        Self {
            buffer,
            destination,
            mappings,
            shutdown_receiver: receiver,
            batch_size,
        }
    }

    async fn process_record(
        &self,
        record: Vec<u8>,
        batch_map: &mut HashMap<String, Vec<Record>>,
        tables: &[TableMetadata],
        metrics: &Metrics,
    ) {
        metrics.increment_records(1).await;
        metrics.increment_bytes(record.len() as u64).await;

        let row_data = RowData::deserialize(record);
        let table_name = row_data.entity.clone(); //self.table_name_map.resolve(&row_data.table);

        let batch = batch_map.entry(table_name.clone()).or_default();
        batch.push(Record::RowData(row_data));

        if batch.len() >= self.batch_size {
            self.flush_all(batch_map, tables).await;
        }
    }

    async fn flush_all(
        &self,
        batch_map: &mut HashMap<String, Vec<Record>>,
        tables: &[TableMetadata],
    ) {
        // return; // TEMPORARY: Disable writing to destination

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

    async fn send_final_report(&self, metrics: &Metrics) {
        let (records_processed, bytes_transferred) = metrics.get_metrics().await;
        let report = MetricsReport::new(records_processed, bytes_transferred, "succeeded".into());
        if let Err(e) = send_report(report.clone()).await {
            error!("Failed to send final report: {}", e);
            let report_json = serde_json::to_string(&report)
                .unwrap_or_else(|_| "Failed to serialize report".to_string());
            error!(
                "All attempts to send report failed. Final Report: {}",
                report_json
            );
        }
    }
}
