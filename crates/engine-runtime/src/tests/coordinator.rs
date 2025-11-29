#[cfg(test)]
mod tests {
    use crate::actor::coordinator::PipelineCoordinator;
    use async_trait::async_trait;
    use engine_core::metrics::Metrics;
    use engine_processing::{
        consumer::{ConsumerStatus, DataConsumer},
        error::{ConsumerError, ProducerError},
        producer::{DataProducer, ProducerStatus},
    };
    use tokio_util::sync::CancellationToken;

    // Mock Producer for testing
    struct MockProducer {
        tick_count: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    impl MockProducer {
        fn new() -> Self {
            Self {
                tick_count: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            }
        }
    }

    #[async_trait]
    impl DataProducer for MockProducer {
        async fn resume(
            &mut self,
            _run_id: &str,
            _item_id: &str,
            _part_id: &str,
        ) -> Result<(), ProducerError> {
            Ok(())
        }

        async fn start_snapshot(&mut self) -> Result<(), ProducerError> {
            Ok(())
        }

        async fn start_cdc(&mut self) -> Result<(), ProducerError> {
            Ok(())
        }

        async fn tick(&mut self) -> Result<ProducerStatus, ProducerError> {
            let count = self
                .tick_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

            // After 3 ticks, finish
            if count >= 2 {
                Ok(ProducerStatus::Finished)
            } else {
                Ok(ProducerStatus::Working)
            }
        }

        async fn stop(&mut self) -> Result<(), ProducerError> {
            Ok(())
        }
    }

    // Mock Consumer for testing
    struct MockConsumer {
        tick_count: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    impl MockConsumer {
        fn new() -> Self {
            Self {
                tick_count: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            }
        }
    }

    #[async_trait]
    impl DataConsumer for MockConsumer {
        async fn start(&mut self) -> Result<(), ConsumerError> {
            Ok(())
        }

        async fn resume(
            &mut self,
            _run_id: &str,
            _item_id: &str,
            _part_id: &str,
        ) -> Result<(), ConsumerError> {
            Ok(())
        }

        async fn tick(&mut self) -> Result<ConsumerStatus, ConsumerError> {
            let count = self
                .tick_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

            // After 3 ticks, finish
            if count >= 2 {
                Ok(ConsumerStatus::Finished)
            } else {
                Ok(ConsumerStatus::Working)
            }
        }

        async fn stop(&mut self) -> Result<(), ConsumerError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_pipeline_coordinator_initialization() {
        let producer = Box::new(MockProducer::new());
        let consumer = Box::new(MockConsumer::new());
        let metrics = Metrics::new();
        let cancel_token = CancellationToken::new();

        let coordinator = PipelineCoordinator::new(producer, consumer, metrics, cancel_token);

        // Initialize should succeed
        assert!(coordinator.initialize().await.is_ok());
    }

    #[tokio::test]
    async fn test_pipeline_coordinator_snapshot_pipeline() {
        let producer = Box::new(MockProducer::new());
        let consumer = Box::new(MockConsumer::new());
        let metrics = Metrics::new();
        let cancel_token = CancellationToken::new();

        let coordinator = PipelineCoordinator::new(producer, consumer, metrics, cancel_token);

        // Initialize
        coordinator.initialize().await.unwrap();

        // Start snapshot pipeline
        let result = coordinator
            .start_snapshot_pipeline(
                "test-run".to_string(),
                "test-item".to_string(),
                "part-0".to_string(),
            )
            .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_pipeline_coordinator_stop() {
        let producer = Box::new(MockProducer::new());
        let consumer = Box::new(MockConsumer::new());
        let metrics = Metrics::new();
        let cancel_token = CancellationToken::new();

        let coordinator =
            PipelineCoordinator::new(producer, consumer, metrics, cancel_token.clone());

        // Initialize
        coordinator.initialize().await.unwrap();

        // Stop should succeed
        assert!(
            coordinator
                .stop("test-run".to_string(), "test-item".to_string())
                .await
                .is_ok()
        );

        // Cancel token should be triggered
        assert!(cancel_token.is_cancelled());
    }

    #[tokio::test]
    async fn test_pipeline_coordinator_accessors() {
        let producer = Box::new(MockProducer::new());
        let consumer = Box::new(MockConsumer::new());
        let metrics = Metrics::new();
        let cancel_token = CancellationToken::new();

        let coordinator = PipelineCoordinator::new(producer, consumer, metrics, cancel_token);

        // Should be able to get actor refs
        assert_eq!(coordinator.producer_ref().name(), "producer");
        assert_eq!(coordinator.consumer_ref().name(), "consumer");

        // Should not be cancelled initially
        assert!(!coordinator.is_cancelled());
    }
}
