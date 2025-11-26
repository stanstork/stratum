use crate::{
    error::ProducerError,
    producer::validation::steps::{ValidationContext, ValidationStep, ValidationStepResult},
};

pub struct ValidationPipeline {
    steps: Vec<Box<dyn ValidationStep>>,
}

impl ValidationPipeline {
    pub fn new() -> Self {
        Self { steps: Vec::new() }
    }

    pub fn add_step(mut self, step: impl ValidationStep + 'static) -> Self {
        self.steps.push(Box::new(step));
        self
    }

    pub async fn execute(
        &self,
        context: &ValidationContext,
    ) -> Result<Vec<ValidationStepResult>, ProducerError> {
        let mut results = Vec::new();
        for step in &self.steps {
            results.push(step.validate(context).await?);
        }
        Ok(results)
    }
}
