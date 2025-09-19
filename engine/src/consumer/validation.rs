use crate::error::ConsumerError;
use async_trait::async_trait;
use tracing::info;

pub struct ValidationConsumer;

impl ValidationConsumer {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl crate::consumer::DataConsumer for ValidationConsumer {
    async fn run(&mut self) -> Result<(), ConsumerError> {
        info!("Running in validation mode. No data will be written.");
        Ok(())
    }
}
