use crate::builder::{
    analysis::{
        registry::{AnalysisState, PipelineAnalysisInput},
        {AnalysisContext, AnalyzerResult, PipelineAnalysisStage, PlanAnalyzer},
    },
    analyzers::source::SourceAnalyzer,
    infra::metadata_cache::MetadataCache,
};
use async_trait::async_trait;
use std::sync::Arc;

pub struct SourceStage {
    pub source_cache: Arc<MetadataCache>,
}

#[async_trait]
impl PipelineAnalysisStage for SourceStage {
    fn name(&self) -> &'static str {
        "source"
    }

    async fn run(
        &self,
        input: &PipelineAnalysisInput,
        ctx: &AnalysisContext,
        state: &mut AnalysisState,
    ) -> AnalyzerResult<()> {
        let analyzer = SourceAnalyzer::new(
            Arc::clone(&self.source_cache),
            input.core_source.as_ref().clone(),
        );
        let source_plan = PlanAnalyzer::analyze(&analyzer, &input.pipeline.source, ctx).await?;
        state.source = Some(source_plan);
        Ok(())
    }
}
