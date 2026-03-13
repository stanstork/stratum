use crate::builder::{
    analysis::{
        registry::{AnalysisState, PipelineAnalysisInput},
        {AnalysisContext, AnalyzerResult, PipelineAnalysisStage, PlanAnalyzer},
    },
    analyzers::destination::DestinationAnalyzer,
};
use async_trait::async_trait;
use engine_processing::io::driver::SchemaDriver;

pub struct DestinationStage {
    pub analyzer: DestinationAnalyzer,
}

#[async_trait]
impl<S: SchemaDriver, D: SchemaDriver> PipelineAnalysisStage<S, D> for DestinationStage {
    fn name(&self) -> &'static str {
        "destination"
    }

    async fn run(
        &self,
        input: &PipelineAnalysisInput,
        ctx: &AnalysisContext<S, D>,
        state: &mut AnalysisState,
    ) -> AnalyzerResult<()> {
        let destination_plan =
            PlanAnalyzer::analyze(&self.analyzer, &input.pipeline.destination, ctx).await?;
        state.destination = Some(destination_plan);
        Ok(())
    }
}
