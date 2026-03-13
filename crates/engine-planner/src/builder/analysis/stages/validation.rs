use crate::builder::{
    analysis::{
        registry::{AnalysisState, PipelineAnalysisInput},
        {AnalysisContext, AnalyzerResult, PipelineAnalysisStage, PlanAnalyzer},
    },
    analyzers::validation::ValidationAnalyzer,
};
use async_trait::async_trait;
use engine_processing::io::driver::SchemaDriver;

pub struct ValidationStage {
    pub analyzer: ValidationAnalyzer,
}

#[async_trait]
impl<S: SchemaDriver, D: SchemaDriver> PipelineAnalysisStage<S, D> for ValidationStage {
    fn name(&self) -> &'static str {
        "validation"
    }

    async fn run(
        &self,
        input: &PipelineAnalysisInput,
        ctx: &AnalysisContext<S, D>,
        state: &mut AnalysisState,
    ) -> AnalyzerResult<()> {
        let validations =
            PlanAnalyzer::analyze(&self.analyzer, input.pipeline.as_ref(), ctx).await?;
        state.validations = Some(validations);
        Ok(())
    }
}
