use crate::builder::{
    analysis::{
        registry::{AnalysisState, PipelineAnalysisInput},
        {AnalysisContext, AnalyzerResult, PipelineAnalysisStage, PlanAnalyzer},
    },
    analyzers::schema::SchemaAnalyzer,
};
use async_trait::async_trait;

pub struct SchemaStage {
    pub analyzer: SchemaAnalyzer,
}

#[async_trait]
impl PipelineAnalysisStage for SchemaStage {
    fn name(&self) -> &'static str {
        "schema"
    }

    async fn run(
        &self,
        input: &PipelineAnalysisInput,
        ctx: &AnalysisContext,
        state: &mut AnalysisState,
    ) -> AnalyzerResult<()> {
        let schema_changes =
            PlanAnalyzer::analyze(&self.analyzer, input.pipeline.as_ref(), ctx).await?;
        state.schema_changes = Some(schema_changes);
        Ok(())
    }
}
