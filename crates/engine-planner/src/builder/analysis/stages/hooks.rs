use crate::builder::{
    analysis::{
        registry::{AnalysisState, PipelineAnalysisInput},
        {AnalysisContext, AnalyzerResult, PipelineAnalysisStage, PlanAnalyzer},
    },
    analyzers::hooks::HooksAnalyzer,
};
use async_trait::async_trait;
use engine_processing::io::driver::SchemaDriver;

pub struct HooksStage<D: SchemaDriver> {
    pub analyzer: HooksAnalyzer<D>,
}

#[async_trait]
impl<S: SchemaDriver, D: SchemaDriver> PipelineAnalysisStage<S, D> for HooksStage<D> {
    fn name(&self) -> &'static str {
        "hooks"
    }

    async fn run(
        &self,
        input: &PipelineAnalysisInput,
        ctx: &AnalysisContext<S, D>,
        state: &mut AnalysisState,
    ) -> AnalyzerResult<()> {
        let hooks = PlanAnalyzer::analyze(&self.analyzer, input.pipeline.as_ref(), ctx).await?;
        state.hooks = Some(hooks);
        Ok(())
    }
}
