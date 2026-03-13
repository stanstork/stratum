use crate::builder::{
    analysis::{
        registry::{AnalysisState, PipelineAnalysisInput},
        {AnalysisContext, AnalyzerResult, PipelineAnalysisStage, PlanAnalyzer},
    },
    analyzers::source::SourceAnalyzer,
    infra::metadata_cache::MetadataCache,
};
use async_trait::async_trait;
use engine_processing::io::driver::SchemaDriver;
use std::sync::Arc;

pub struct SourceStage<S: SchemaDriver> {
    pub source_cache: Arc<MetadataCache<S>>,
}

#[async_trait]
impl<S: SchemaDriver, D: SchemaDriver> PipelineAnalysisStage<S, D> for SourceStage<S> {
    fn name(&self) -> &'static str {
        "source"
    }

    async fn run(
        &self,
        input: &PipelineAnalysisInput,
        ctx: &AnalysisContext<S, D>,
        state: &mut AnalysisState,
    ) -> AnalyzerResult<()> {
        let analyzer = SourceAnalyzer::new(Arc::clone(&self.source_cache));
        let source_plan = PlanAnalyzer::analyze(&analyzer, &input.pipeline.source, ctx).await?;
        state.source = Some(source_plan);
        Ok(())
    }
}
