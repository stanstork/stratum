use crate::error::ProducerError;
use async_trait::async_trait;
use engine_config::report::finding::Finding;

pub mod fast_path;
pub mod sampling;
pub mod schema;
pub mod sql_gen;

#[derive(Debug, Clone)]
pub struct ValidationContext {
    // Shared context across steps
}

#[derive(Debug, Clone)]
pub struct ValidationStepResult {
    pub findings: Vec<Finding>,
    pub metadata: serde_json::Value,
}

#[async_trait]
pub trait ValidationStep: Send + Sync {
    async fn validate(
        &self,
        context: &ValidationContext,
    ) -> Result<ValidationStepResult, ProducerError>;

    fn name(&self) -> &str;
}
