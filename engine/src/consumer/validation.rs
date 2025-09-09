use async_trait::async_trait;

pub struct ValidationConsumer;

impl ValidationConsumer {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl crate::consumer::DataConsumer for ValidationConsumer {
    async fn run(&self) {
        // Validation consumer does nothing, since no data needs to be consumed.
        return;
    }
}
