use crate::builder::{
    analysis::{
        registry::{AnalysisState, PipelineAnalysisInput},
        {AnalysisContext, AnalyzerResult, PipelineAnalysisStage, PlanAnalyzer},
    },
    analyzers::join::JoinAnalyzer,
};
use async_trait::async_trait;

pub struct JoinStage {
    pub analyzer: JoinAnalyzer,
}

#[async_trait]
impl PipelineAnalysisStage for JoinStage {
    fn name(&self) -> &'static str {
        "join"
    }

    async fn run(
        &self,
        input: &PipelineAnalysisInput,
        ctx: &AnalysisContext,
        state: &mut AnalysisState,
    ) -> AnalyzerResult<()> {
        let joins =
            PlanAnalyzer::analyze(&self.analyzer, &input.pipeline.source.joins, ctx).await?;
        state.joins = Some(joins);
        Ok(())
    }
}
