use crate::error::ConsumerError;
use async_trait::async_trait;
use tracing::info;

pub struct ValidationConsumer;

impl Default for ValidationConsumer {
    fn default() -> Self {
        Self::new()
    }
}

impl ValidationConsumer {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl crate::consumer::DataConsumer for ValidationConsumer {
    async fn run(&mut self) -> Result<(), ConsumerError> {
        info!("Running in validation mode. No data will be written.");

        loop {
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            info!("ValidationConsumer is alive...");
        }
    }
}
