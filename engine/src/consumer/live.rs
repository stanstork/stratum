use crate::{
    buffer::SledBuffer,
    consumer::{trigger::TriggerGuard, DataConsumer},
    destination::{data::DataDestination, Destination},
    error::ConsumerError,
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
    batch_map: HashMap<String, Vec<Record>>,
}

#[async_trait]
impl DataConsumer for LiveConsumer {
    async fn run(&mut self) -> Result<(), ConsumerError> {
        let tables = match &self.destination.data_dest {
            DataDestination::Database(db) => db.lock().await.tables(),
        };

        // Guard to ensure triggers are restored on exit
        let _trigger_guard = TriggerGuard::new(&self.destination, &tables, false).await?;

        let metrics = Metrics::new();

        loop {
            match self.buffer.read_next() {
                Some(record) => {
                    self.process_record(record, &tables, &metrics).await?;
                }
                None => {
                    self.flush_all(&tables).await?;

                    // If the shutdown signal is received, it means the producer has finished
                    // processing all records and the consumer can safely exit if the buffer is empty
                    if *self.shutdown_receiver.borrow() {
                        info!("Shutdown signal received and buffer is empty. Exiting.");
                        break;
                    }

                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
        }

        self.send_final_report(&metrics).await;
        info!("Consumer finished");
        Ok(())
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
            batch_map: HashMap::new(),
        }
    }

    async fn process_record(
        &mut self,
        record: Vec<u8>,
        tables: &[TableMetadata],
        metrics: &Metrics,
    ) -> Result<(), ConsumerError> {
        metrics.increment_records(1).await;
        metrics.increment_bytes(record.len() as u64).await;

        let row_data = RowData::deserialize(record);
        let table_name = row_data.entity.clone(); //self.table_name_map.resolve(&row_data.table);

        let batch = self.batch_map.entry(table_name.clone()).or_default();
        batch.push(Record::RowData(row_data));

        if batch.len() >= self.batch_size {
            self.flush_all(tables).await?;
        }
        Ok(())
    }

    async fn flush_all(&mut self, tables: &[TableMetadata]) -> Result<(), ConsumerError> {
        for table in tables.iter() {
            // Get the table name from the map or use the original name if no mapping is found
            // This is needed when the source and destination table names are different
            let table_name = self.mappings.entity_name_map.resolve(&table.name);

            if let Some(records) = self.batch_map.remove(&table_name) {
                if records.is_empty() {
                    continue;
                }

                let start_time = Instant::now();

                self.destination
                    .write_batch(table, records)
                    .await
                    .map_err(|e| ConsumerError::WriteBatch {
                        table: table.name.clone(),
                        source: Box::new(e),
                    })?;

                let elapsed = start_time.elapsed().as_millis();
                info!(
                    table = %table.name,
                    duration_ms = elapsed,
                    "Batch written successfully."
                );
            }
        }
        Ok(())
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
