use crate::builder::{
    analysis::{
        registry::{AnalysisState, PipelineAnalysisInput},
        {AnalysisContext, AnalyzerResult, PipelineAnalysisStage, PlanAnalyzer},
    },
    analyzers::hooks::HooksAnalyzer,
};
use async_trait::async_trait;

pub struct HooksStage {
    pub analyzer: HooksAnalyzer,
}

#[async_trait]
impl PipelineAnalysisStage for HooksStage {
    fn name(&self) -> &'static str {
        "hooks"
    }

    async fn run(
        &self,
        input: &PipelineAnalysisInput,
        ctx: &AnalysisContext,
        state: &mut AnalysisState,
    ) -> AnalyzerResult<()> {
        let hooks = PlanAnalyzer::analyze(&self.analyzer, input.pipeline.as_ref(), ctx).await?;
        state.hooks = Some(hooks);
        Ok(())
    }
}
