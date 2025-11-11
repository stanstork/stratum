use crate::{
    consumer::{DataConsumer, trigger::TriggerGuard},
    error::ConsumerError,
};
use async_trait::async_trait;
use connectors::sql::base::metadata::table::TableMetadata;
use engine_config::report::metrics::{MetricsReport, send_report};
use engine_core::{
    connectors::destination::{DataDestination, Destination},
    metrics::Metrics,
    state::buffer::SledBuffer,
};
use model::{
    records::{
        batch::Batch,
        record::{DataRecord, Record},
        row::RowData,
    },
    transform::mapping::EntityMapping,
};
use std::{collections::HashMap, sync::Arc, time::Instant};
use tokio::sync::{mpsc, watch::Receiver};
use tracing::{error, info};

pub struct LiveConsumer {
    batch_rx: mpsc::Receiver<Batch>,
    destination: Destination,
    mappings: EntityMapping,
    shutdown_receiver: Receiver<bool>,
}

#[async_trait]
impl DataConsumer for LiveConsumer {
    async fn run(&mut self) -> Result<(), ConsumerError> {
        let tables = match &self.destination.data_dest {
            DataDestination::Database(db) => db.data.lock().await.tables(),
        };

        // Guard to ensure triggers are restored on exit
        let _trigger_guard = TriggerGuard::new(&self.destination, &tables, false).await?;

        let metrics = Metrics::new();

        while let Some(batch) = self.batch_rx.recv().await {
            let table = &tables[0];
            match &self.destination.data_dest {
                DataDestination::Database(db) => {
                    db.sink.write_fast_path(table, &batch).await.unwrap();
                    todo!("Implement fast path write logic")
                }
            }
        }

        // loop {
        //     match self.buffer.read_next() {
        //         Some(record) => {
        //             self.process_record(record, &tables, &metrics).await?;
        //         }
        //         None => {
        //             self.flush_all(&tables).await?;

        //             // If the shutdown signal is received, it means the producer has finished
        //             // processing all records and the consumer can safely exit if the buffer is empty
        //             if *self.shutdown_receiver.borrow() {
        //                 info!("Shutdown signal received and buffer is empty. Exiting.");
        //                 break;
        //             }

        //             tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        //         }
        //     }
        // }

        // self.send_final_report(&metrics).await;
        info!("Consumer finished");
        Ok(())
    }
}

impl LiveConsumer {
    pub fn new(
        batch_rx: mpsc::Receiver<Batch>,
        destination: Destination,
        mappings: EntityMapping,
        receiver: Receiver<bool>,
    ) -> Self {
        Self {
            batch_rx,
            destination,
            mappings,
            shutdown_receiver: receiver,
        }
    }

    async fn process_record(
        &mut self,
        record: Vec<u8>,
        tables: &[TableMetadata],
        metrics: &Metrics,
    ) -> Result<(), ConsumerError> {
        // metrics.increment_records(1).await;
        // metrics.increment_bytes(record.len() as u64).await;

        // let row_data = RowData::deserialize(record);
        // let table_name = row_data.entity.clone(); //self.table_name_map.resolve(&row_data.table);

        // let batch = self.batch_map.entry(table_name.clone()).or_default();
        // batch.push(Record::RowData(row_data));

        // if batch.len() >= self.batch_size {
        //     self.flush_all(tables).await?;
        // }
        Ok(())
    }

    async fn flush_all(&mut self, tables: &[TableMetadata]) -> Result<(), ConsumerError> {
        // for table in tables.iter() {
        //     // Get the table name from the map or use the original name if no mapping is found
        //     // This is needed when the source and destination table names are different
        //     let table_name = self.mappings.entity_name_map.resolve(&table.name);

        //     if let Some(records) = self.batch_map.remove(&table_name) {
        //         if records.is_empty() {
        //             continue;
        //         }

        //         let start_time = Instant::now();

        //         self.destination
        //             .write_batch(table, records)
        //             .await
        //             .map_err(|e| ConsumerError::WriteBatch {
        //                 table: table.name.clone(),
        //                 source: Box::new(e),
        //             })?;

        //         let elapsed = start_time.elapsed().as_millis();
        //         info!(
        //             table = %table.name,
        //             duration_ms = elapsed,
        //             "Batch written successfully."
        //         );
        //     }
        // }
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
