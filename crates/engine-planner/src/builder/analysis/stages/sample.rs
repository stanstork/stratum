use crate::{
    builder::{
        analysis::{
            registry::{AnalysisState, PipelineAnalysisInput},
            {AnalysisContext, AnalyzerResult, PipelineAnalysisStage, PlanAnalyzer},
        },
        analyzers::sample::SampleCollector,
    },
    plan::sample::preview::SampleDataPreview,
};
use async_trait::async_trait;
use std::sync::Arc;

pub struct SampleStage;

#[async_trait]
impl PipelineAnalysisStage for SampleStage {
    fn name(&self) -> &'static str {
        "sample"
    }

    async fn run(
        &self,
        input: &PipelineAnalysisInput,
        ctx: &AnalysisContext,
        state: &mut AnalysisState,
    ) -> AnalyzerResult<()> {
        // Skip sample collection if not enabled
        if !input.sample_config.enabled {
            state.sample = Some(SampleDataPreview::default());
            return Ok(());
        }

        let validations = state.require_validations()?.clone();
        let sample_collector =
            SampleCollector::new(Arc::clone(&input.core_source), input.sample_config.clone());
        let sample = PlanAnalyzer::analyze(
            &sample_collector,
            &(
                (*input.pipeline).clone(),
                (*ctx.mapping).clone(),
                validations,
                input.mapped_columns_only,
            ),
            ctx,
        )
        .await?;
        state.sample = Some(sample);
        Ok(())
    }
}
