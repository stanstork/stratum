use crate::builder::{
    analysis::{
        registry::{AnalysisState, PipelineAnalysisInput},
        {AnalysisContext, AnalyzerResult, PipelineAnalysisStage, PlanAnalyzer},
    },
    analyzers::mapping::MappingAnalyzer,
};
use async_trait::async_trait;
use engine_processing::io::driver::SchemaDriver;

pub struct MappingStage {
    pub analyzer: MappingAnalyzer,
}

#[async_trait]
impl<S: SchemaDriver, D: SchemaDriver> PipelineAnalysisStage<S, D> for MappingStage {
    fn name(&self) -> &'static str {
        "mapping"
    }

    async fn run(
        &self,
        input: &PipelineAnalysisInput,
        ctx: &AnalysisContext<S, D>,
        state: &mut AnalysisState,
    ) -> AnalyzerResult<()> {
        let source_plan = state.require_source()?.clone();
        let mappings = PlanAnalyzer::analyze(
            &self.analyzer,
            &(input.pipeline.transformations.clone(), source_plan.clone()),
            ctx,
        )
        .await?;

        state.mappings = Some(mappings);
        Ok(())
    }
}
