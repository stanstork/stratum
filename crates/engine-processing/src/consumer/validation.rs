use crate::{
    consumer::{ConsumerStatus, DataConsumer},
    error::ConsumerError,
};
use async_trait::async_trait;
use model::records::batch::Batch;
use tokio::sync::mpsc;
use tracing::info;

/// Consumer for dry run validation mode - receives batches but doesn't write them
pub struct ValidationConsumer {
    batch_rx: mpsc::Receiver<Batch>,
    batches_processed: usize,
    records_processed: usize,
}

impl ValidationConsumer {
    pub fn new(batch_rx: mpsc::Receiver<Batch>) -> Self {
        Self {
            batch_rx,
            batches_processed: 0,
            records_processed: 0,
        }
    }
}

#[async_trait]
impl DataConsumer for ValidationConsumer {
    async fn start(&mut self) -> Result<(), ConsumerError> {
        info!("Starting validation consumer - dry run mode (no data will be written)");
        Ok(())
    }

    async fn resume(
        &mut self,
        _run_id: &str,
        _item_id: &str,
        _part_id: &str,
    ) -> Result<(), ConsumerError> {
        // Resume not supported in validation mode
        Ok(())
    }

    async fn tick(&mut self) -> Result<ConsumerStatus, ConsumerError> {
        match self.batch_rx.try_recv() {
            Ok(batch) => {
                let row_count = batch.rows.len();
                self.batches_processed += 1;
                self.records_processed += row_count;
                info!(
                    "ValidationConsumer: received batch {} with {} records (total: {} batches, {} records)",
                    self.batches_processed,
                    row_count,
                    self.batches_processed,
                    self.records_processed
                );
                Ok(ConsumerStatus::Working)
            }
            Err(mpsc::error::TryRecvError::Empty) => Ok(ConsumerStatus::Idle),
            Err(mpsc::error::TryRecvError::Disconnected) => {
                info!(
                    "ValidationConsumer: channel closed. Processed {} batches with {} total records",
                    self.batches_processed, self.records_processed
                );
                Ok(ConsumerStatus::Finished)
            }
        }
    }

    async fn stop(&mut self) -> Result<(), ConsumerError> {
        info!(
            "ValidationConsumer stopped. Total: {} batches, {} records",
            self.batches_processed, self.records_processed
        );
        Ok(())
    }

    fn rows_written(&self) -> u64 {
        self.records_processed as u64
    }
}
