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
use engine_processing::io::driver::SchemaDriver;
use std::sync::Arc;

pub struct SampleStage;

#[async_trait]
impl<S: SchemaDriver, D: SchemaDriver> PipelineAnalysisStage<S, D> for SampleStage {
    fn name(&self) -> &'static str {
        "sample"
    }

    async fn run(
        &self,
        input: &PipelineAnalysisInput,
        ctx: &AnalysisContext<S, D>,
        state: &mut AnalysisState,
    ) -> AnalyzerResult<()> {
        // Skip sample collection if not enabled
        if !input.sample_config.enabled {
            state.sample = Some(SampleDataPreview::default());
            return Ok(());
        }

        let validations = state.require_validations()?.clone();
        let sample_collector =
            SampleCollector::new(Arc::clone(&ctx.src_driver), input.sample_config.clone());
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
