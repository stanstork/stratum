use crate::builder::{
    analysis::{
        registry::{AnalysisState, PipelineAnalysisInput},
        {AnalysisContext, AnalyzerResult, PipelineAnalysisStage, PlanAnalyzer},
    },
    analyzers::pagination::PaginationAnalyzer,
};
use async_trait::async_trait;
use engine_processing::io::driver::SchemaDriver;

pub struct PaginationStage {
    pub analyzer: PaginationAnalyzer,
}

#[async_trait]
impl<S: SchemaDriver, D: SchemaDriver> PipelineAnalysisStage<S, D> for PaginationStage {
    fn name(&self) -> &'static str {
        "pagination"
    }

    async fn run(
        &self,
        input: &PipelineAnalysisInput,
        ctx: &AnalysisContext<S, D>,
        state: &mut AnalysisState,
    ) -> AnalyzerResult<()> {
        let pagination = PlanAnalyzer::analyze(
            &self.analyzer,
            &(
                input.pipeline.source.table.clone(),
                input.pipeline.source.pagination.clone(),
            ),
            ctx,
        )
        .await?;
        state.pagination = Some(pagination);
        Ok(())
    }
}
