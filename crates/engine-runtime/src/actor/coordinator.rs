use super::{consumer::ConsumerActor, producer::ProducerActor};
use crate::{
    actor::{
        ActorRef,
        messages::{ConsumerMsg, ProducerMsg},
        spawn::spawn_actor,
    },
    error::ActorError,
};
use engine_core::{event_bus::bus::EventBus, metrics::Metrics};
use engine_processing::{consumer::DataConsumer, producer::DataProducer};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

/// Coordinates the Producer and Consumer actors in a data pipeline.
pub struct PipelineCoordinator {
    producer_ref: ActorRef<ProducerMsg>,
    consumer_ref: ActorRef<ConsumerMsg>,
    producer_handle: JoinHandle<()>,
    consumer_handle: JoinHandle<()>,
    cancel_token: CancellationToken,
}

impl PipelineCoordinator {
    pub fn new(
        producer: Box<dyn DataProducer + Send + 'static>,
        consumer: Box<dyn DataConsumer + Send + 'static>,
        metrics: Metrics,
        cancel_token: CancellationToken,
    ) -> Self {
        // Create and spawn producer actor
        let producer_actor = ProducerActor::new(producer, cancel_token.clone(), metrics.clone());
        let (producer_ref, producer_handle) = spawn_actor("producer", 100, producer_actor);

        // Create and spawn consumer actor
        let consumer_actor = ConsumerActor::new(consumer, cancel_token.clone(), metrics);
        let (consumer_ref, consumer_handle) = spawn_actor("consumer", 100, consumer_actor);

        Self {
            producer_ref,
            consumer_ref,
            producer_handle,
            consumer_handle,
            cancel_token,
        }
    }

    /// Initializes the actors by setting their self-references.
    pub async fn initialize(&self) -> Result<(), ActorError> {
        self.producer_ref
            .send(ProducerMsg::SetActorRef(self.producer_ref.clone()))
            .await?;
        self.consumer_ref
            .send(ConsumerMsg::SetActorRef(self.consumer_ref.clone()))
            .await?;

        Ok(())
    }

    /// Sets the EventBus for both producer and consumer actors.
    pub async fn set_event_bus(&self, event_bus: EventBus) -> Result<(), ActorError> {
        self.producer_ref
            .send(ProducerMsg::SetEventBus(event_bus.clone()))
            .await?;

        self.consumer_ref
            .send(ConsumerMsg::SetEventBus(event_bus))
            .await?;

        Ok(())
    }

    /// Starts the producer for snapshot processing.
    pub async fn start_snapshot(&self, run_id: String, item_id: String) -> Result<(), ActorError> {
        info!(run_id = %run_id, item_id = %item_id, "Starting snapshot");

        self.producer_ref
            .send(ProducerMsg::StartSnapshot {
                run_id: run_id.clone(),
                item_id: item_id.clone(),
            })
            .await?;

        Ok(())
    }

    /// Starts the consumer for a specific partition.
    pub async fn start_consumer(
        &self,
        run_id: String,
        item_id: String,
        part_id: String,
    ) -> Result<(), ActorError> {
        self.consumer_ref
            .send(ConsumerMsg::Start {
                run_id,
                item_id,
                part_id,
            })
            .await?;

        Ok(())
    }

    /// Starts the producer for CDC processing.
    pub async fn start_cdc(&self, run_id: String, item_id: String) -> Result<(), ActorError> {
        info!(run_id = %run_id, item_id = %item_id, "Starting CDC");

        self.producer_ref
            .send(ProducerMsg::StartCdc { run_id, item_id })
            .await?;

        Ok(())
    }

    /// Flushes the consumer to process any remaining batches.
    pub async fn flush_consumer(&self, run_id: String, item_id: String) -> Result<(), ActorError> {
        info!(run_id = %run_id, item_id = %item_id, "Flushing consumer");

        self.consumer_ref
            .send(ConsumerMsg::Flush { run_id, item_id })
            .await?;

        Ok(())
    }

    /// Gracefully stops the pipeline.
    pub async fn stop(&self, run_id: String, item_id: String) -> Result<(), ActorError> {
        info!("Stopping pipeline");

        self.cancel_token.cancel();

        if let Err(e) = self
            .producer_ref
            .send(ProducerMsg::Stop {
                run_id: run_id.clone(),
                item_id: item_id.clone(),
            })
            .await
        {
            error!(error = ?e, "Failed to send stop to producer");
        }

        if let Err(e) = self
            .consumer_ref
            .send(ConsumerMsg::Stop { run_id, item_id })
            .await
        {
            error!(error = ?e, "Failed to send stop to consumer");
        }

        info!("Pipeline stopped");
        Ok(())
    }

    /// Waits for both actors to complete.
    pub async fn wait(self) -> Result<(), ActorError> {
        // Drop actor references to allow mailboxes to close when actors finish
        // This is necessary because actors hold self-references that they drop when done,
        // but the coordinator also holds references that must be dropped for termination
        drop(self.producer_ref);
        drop(self.consumer_ref);

        // Wait for both actors to finish
        let _ = tokio::join!(self.producer_handle, self.consumer_handle);

        Ok(())
    }

    pub fn producer_ref(&self) -> &ActorRef<ProducerMsg> {
        &self.producer_ref
    }

    pub fn consumer_ref(&self) -> &ActorRef<ConsumerMsg> {
        &self.consumer_ref
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
            "Starting CDC pipeline"
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
