use super::{consumer::run_consumer, producer::run_producer};
use crate::{
    actor::messages::{ConsumerMsg, ProducerMsg},
    error::ActorError,
};
use engine_core::{event_bus::bus::EventBus, metrics::Metrics};
use engine_processing::{consumer::Consumer, producer::Producer};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

/// Coordinates the Producer and Consumer tasks in a data pipeline.
pub struct PipelineCoordinator {
    producer_tx: mpsc::Sender<ProducerMsg>,
    consumer_tx: mpsc::Sender<ConsumerMsg>,
    producer_handle: JoinHandle<Result<(), ActorError>>,
    consumer_handle: JoinHandle<Result<(), ActorError>>,
    cancel_token: CancellationToken,
}

impl PipelineCoordinator {
    pub fn new(
        producer: Producer,
        consumer: Consumer,
        metrics: Metrics,
        cancel_token: CancellationToken,
        event_bus: EventBus,
    ) -> Self {
        let (producer_tx, producer_rx) = mpsc::channel::<ProducerMsg>(100);
        let (consumer_tx, consumer_rx) = mpsc::channel::<ConsumerMsg>(100);

        let producer_handle = tokio::spawn(run_producer(
            producer,
            producer_rx,
            cancel_token.clone(),
            event_bus.clone(),
            metrics.clone(),
        ));

        let consumer_handle = tokio::spawn(run_consumer(
            consumer,
            consumer_rx,
            cancel_token.clone(),
            event_bus,
            metrics,
        ));

        Self {
            producer_tx,
            consumer_tx,
            producer_handle,
            consumer_handle,
            cancel_token,
        }
    }

    /// Starts the producer for snapshot processing.
    pub async fn start_snapshot(&self, run_id: String, item_id: String) -> Result<(), ActorError> {
        info!(run_id = %run_id, item_id = %item_id, "starting snapshot producer");

        self.producer_tx
            .send(ProducerMsg::StartSnapshot {
                run_id: run_id.clone(),
                item_id: item_id.clone(),
            })
            .await
            .map_err(|_| ActorError::MailboxClosed)?;

        Ok(())
    }

    /// Starts the consumer for a specific partition.
    pub async fn start_consumer(
        &self,
        run_id: String,
        item_id: String,
        part_id: String,
    ) -> Result<(), ActorError> {
        self.consumer_tx
            .send(ConsumerMsg::Start {
                run_id,
                item_id,
                part_id,
            })
            .await
            .map_err(|_| ActorError::MailboxClosed)?;

        Ok(())
    }

    /// Starts the producer for CDC processing.
    pub async fn start_cdc(&self, run_id: String, item_id: String) -> Result<(), ActorError> {
        info!(run_id = %run_id, item_id = %item_id, "starting CDC producer");

        self.producer_tx
            .send(ProducerMsg::StartCdc { run_id, item_id })
            .await
            .map_err(|_| ActorError::MailboxClosed)?;

        Ok(())
    }

    /// Flushes the consumer to process any remaining batches.
    pub async fn flush_consumer(&self, run_id: String, item_id: String) -> Result<(), ActorError> {
        info!(run_id = %run_id, item_id = %item_id, "flushing consumer");

        self.consumer_tx
            .send(ConsumerMsg::Flush { run_id, item_id })
            .await
            .map_err(|_| ActorError::MailboxClosed)?;

        Ok(())
    }

    /// Gracefully stops the pipeline.
    pub async fn stop(
        &self,
        run_id: String,
        item_id: String,
        part_id: String,
    ) -> Result<(), ActorError> {
        info!(run_id = %run_id, item_id = %item_id, "stopping pipeline");

        self.cancel_token.cancel();

        if let Err(e) = self
            .producer_tx
            .send(ProducerMsg::Stop {
                run_id: run_id.clone(),
                item_id: item_id.clone(),
            })
            .await
        {
            error!(error = ?e, "failed to send stop to producer");
        }

        if let Err(e) = self
            .consumer_tx
            .send(ConsumerMsg::Stop {
                run_id,
                item_id,
                part_id,
            })
            .await
        {
            error!(error = ?e, "failed to send stop to consumer");
        }

        info!("pipeline stopped");
        Ok(())
    }

    /// Waits for both tasks to complete and returns any errors.
    pub async fn wait(self) -> Result<(), ActorError> {
        // Senders are intentionally kept alive for the duration of the join.
        // Dropping them here (before join) would immediately close the channel,
        // causing tasks to receive None on rx.recv() and stop before doing any work.
        // Tasks stop themselves via TickAction::Done when their work is complete;
        // the senders drop naturally when this function returns.
        let _producer_tx = self.producer_tx;
        let _consumer_tx = self.consumer_tx;

        let (producer_join_result, consumer_join_result) =
            tokio::join!(self.producer_handle, self.consumer_handle);

        let producer_result = producer_join_result.map_err(|e| {
            error!(error = %e, "producer task panicked");
            ActorError::Internal(format!("Producer task panicked: {}", e))
        })?;

        let consumer_result = consumer_join_result.map_err(|e| {
            error!(error = %e, "consumer task panicked");
            ActorError::Internal(format!("Consumer task panicked: {}", e))
        })?;

        producer_result?;
        consumer_result?;

        Ok(())
    }

    /// Starts a complete snapshot pipeline: starts both producer and consumer.
    pub async fn start_snapshot_pipeline(
        &self,
        run_id: String,
        item_id: String,
        part_id: String,
    ) -> Result<(), ActorError> {
        // Start consumer first so it's ready to receive data
        self.start_consumer(run_id.clone(), item_id.clone(), part_id)
            .await?;

        // Then start the producer
        self.start_snapshot(run_id, item_id).await?;

        Ok(())
    }

    /// Starts a complete CDC pipeline: starts both producer and consumer.
    pub async fn start_cdc_pipeline(
        &self,
        run_id: String,
        item_id: String,
        part_id: String,
    ) -> Result<(), ActorError> {
        info!(
            run_id = %run_id,
            item_id = %item_id,
            part_id = %part_id,
            "starting CDC pipeline"
        );

        // Start consumer first so it's ready to receive data
        self.start_consumer(run_id.clone(), item_id.clone(), part_id)
            .await?;

        // Then start the CDC producer
        self.start_cdc(run_id, item_id).await?;

        info!("CDC pipeline started");
        Ok(())
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancel_token.is_cancelled()
    }
}
