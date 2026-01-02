use crate::builder::{
    analysis::{
        registry::{AnalysisState, PipelineAnalysisInput},
        {AnalysisContext, AnalyzerResult, PipelineAnalysisStage, PlanAnalyzer},
    },
    analyzers::filter::FilterAnalyzer,
    infra::metadata_cache::MetadataCache,
};
use async_trait::async_trait;
use std::sync::Arc;

pub struct FilterStage {
    pub source_cache: Arc<MetadataCache>,
}

#[async_trait]
impl PipelineAnalysisStage for FilterStage {
    fn name(&self) -> &'static str {
        "filter"
    }

    async fn run(
        &self,
        input: &PipelineAnalysisInput,
        ctx: &AnalysisContext,
        state: &mut AnalysisState,
    ) -> AnalyzerResult<()> {
        let source_plan = state.require_source()?.clone();
        let filter_analyzer = FilterAnalyzer::new(Arc::clone(&self.source_cache));
        let filters = PlanAnalyzer::analyze(
            &filter_analyzer,
            &(input.pipeline.source.clone(), source_plan),
            ctx,
        )
        .await?;
        state.filters = Some(filters);
        Ok(())
    }
}
