use super::{
    stages::*,
    {AnalysisContext, AnalyzerError, AnalyzerResult},
};
use crate::{
    builder::{
        analyzers::{
            destination::DestinationAnalyzer, hooks::HooksAnalyzer, join::JoinAnalyzer,
            mapping::MappingAnalyzer, pagination::PaginationAnalyzer, sample::SampleConfig,
            schema::SchemaAnalyzer, validation::ValidationAnalyzer,
        },
        infra::metadata_cache::MetadataCache,
    },
    plan::{
        hooks::plan::HooksPlan,
        pagination::plan::PaginationPlan,
        pipeline::{destination::DestinationPlan, source::SourcePlan},
        sample::preview::SampleDataPreview,
        schema::change::SchemaChange,
        transform::{filter::FilterPlan, join::JoinPlan, mapping::ColumnMapping},
        validation::plan::ValidationPlan,
    },
};

use async_trait::async_trait;
use engine_processing::io::driver::SchemaDriver;
use model::execution::pipeline::Pipeline;
use std::{sync::Arc, time::Duration};

/// Aggregated results from all analyzers
#[derive(Debug, Clone)]
pub struct AnalysisReport {
    pub pipeline_name: String,
    pub source: SourcePlan,
    pub destination: DestinationPlan,
    pub filters: Option<FilterPlan>,
    pub joins: Vec<JoinPlan>,
    pub mappings: Vec<ColumnMapping>,
    pub validations: Vec<ValidationPlan>,
    pub pagination: Option<PaginationPlan>,
    pub hooks: HooksPlan,
    pub schema_changes: Vec<SchemaChange>,
    pub sample: SampleDataPreview,
}

/// Inputs that vary per pipeline run.
pub struct PipelineAnalysisInput {
    pub pipeline: Arc<Pipeline>,
    pub sample_config: SampleConfig,
    pub mapped_columns_only: bool,
}

impl PipelineAnalysisInput {
    pub fn new(
        pipeline: Arc<Pipeline>,
        sample_config: SampleConfig,
        mapped_columns_only: bool,
    ) -> Self {
        Self {
            pipeline,
            sample_config,
            mapped_columns_only,
        }
    }
}

/// Shared state built up by analysis stages.
pub struct AnalysisState {
    pipeline_name: String,
    pub(crate) source: Option<SourcePlan>,
    pub(crate) destination: Option<DestinationPlan>,
    pub(crate) filters: Option<Option<crate::plan::transform::filter::FilterPlan>>,
    pub(crate) joins: Option<Vec<JoinPlan>>,
    pub(crate) mappings: Option<Vec<ColumnMapping>>,
    pub(crate) validations: Option<Vec<ValidationPlan>>,
    pub(crate) pagination: Option<Option<PaginationPlan>>,
    pub(crate) hooks: Option<HooksPlan>,
    pub(crate) schema_changes: Option<Vec<SchemaChange>>,
    pub(crate) sample: Option<SampleDataPreview>,
}

impl AnalysisState {
    fn new(pipeline_name: String) -> Self {
        Self {
            pipeline_name,
            source: None,
            destination: None,
            filters: None,
            joins: None,
            mappings: None,
            validations: None,
            pagination: None,
            hooks: None,
            schema_changes: None,
            sample: None,
        }
    }

    pub(crate) fn require_source(&self) -> AnalyzerResult<&SourcePlan> {
        self.source.as_ref().ok_or_else(|| {
            AnalyzerError::error("registry", "Missing source analysis result".to_string())
        })
    }

    pub(crate) fn require_validations(&self) -> AnalyzerResult<&Vec<ValidationPlan>> {
        self.validations.as_ref().ok_or_else(|| {
            AnalyzerError::error("registry", "Missing validation analysis result".to_string())
        })
    }

    pub fn build(self) -> AnalyzerResult<AnalysisReport> {
        Ok(AnalysisReport {
            pipeline_name: self.pipeline_name,
            source: self
                .source
                .ok_or_else(|| AnalyzerError::error("registry", "Missing source".to_string()))?,
            destination: self.destination.ok_or_else(|| {
                AnalyzerError::error("registry", "Missing destination".to_string())
            })?,
            filters: self.filters.unwrap_or(None),
            joins: self
                .joins
                .ok_or_else(|| AnalyzerError::error("registry", "Missing joins".to_string()))?,
            mappings: self
                .mappings
                .ok_or_else(|| AnalyzerError::error("registry", "Missing mappings".to_string()))?,
            validations: self.validations.ok_or_else(|| {
                AnalyzerError::error("registry", "Missing validations".to_string())
            })?,
            pagination: self.pagination.unwrap_or(None),
            hooks: self
                .hooks
                .ok_or_else(|| AnalyzerError::error("registry", "Missing hooks".to_string()))?,
            schema_changes: self.schema_changes.ok_or_else(|| {
                AnalyzerError::error("registry", "Missing schema changes".to_string())
            })?,
            sample: self
                .sample
                .ok_or_else(|| AnalyzerError::error("registry", "Missing sample".to_string()))?,
        })
    }
}

/// Planner-style analysis stage for a pipeline.
#[async_trait]
pub trait PipelineAnalysisStage<S: SchemaDriver, D: SchemaDriver>: Send + Sync {
    fn name(&self) -> &'static str;

    async fn run(
        &self,
        input: &PipelineAnalysisInput,
        ctx: &AnalysisContext<S, D>,
        state: &mut AnalysisState,
    ) -> AnalyzerResult<()>;
}

/// Registry that holds and coordinates all analyzers.
pub struct AnalyzerRegistry<S: SchemaDriver, D: SchemaDriver> {
    stages: Vec<Box<dyn PipelineAnalysisStage<S, D>>>,
}

impl<S: SchemaDriver, D: SchemaDriver> AnalyzerRegistry<S, D> {
    pub fn new(
        source_cache: Arc<MetadataCache<S>>,
        schema_plan: Arc<engine_core::schema::plan::SchemaPlan>,
        mapping: &model::transform::mapping::TransformationMetadata,
        dest_driver: Arc<D>,
        _timeout: Duration,
    ) -> Self {
        let stages: Vec<Box<dyn PipelineAnalysisStage<S, D>>> = vec![
            Box::new(SourceStage {
                source_cache: Arc::clone(&source_cache),
            }),
            Box::new(DestinationStage {
                analyzer: DestinationAnalyzer,
            }),
            Box::new(FilterStage {
                source_cache: Arc::clone(&source_cache),
            }),
            Box::new(JoinStage {
                analyzer: JoinAnalyzer,
            }),
            Box::new(MappingStage {
                analyzer: MappingAnalyzer::new(Arc::clone(&schema_plan), mapping),
            }),
            Box::new(JoinUsageStage),
            Box::new(ValidationStage {
                analyzer: ValidationAnalyzer,
            }),
            Box::new(PaginationStage {
                analyzer: PaginationAnalyzer,
            }),
            Box::new(HooksStage {
                analyzer: HooksAnalyzer::new(&dest_driver),
            }),
            Box::new(SchemaStage {
                analyzer: SchemaAnalyzer,
            }),
            Box::new(SampleStage),
        ];

        Self { stages }
    }

    pub fn register_stage(&mut self, stage: Box<dyn PipelineAnalysisStage<S, D>>) {
        self.stages.push(stage);
    }

    pub async fn analyze_pipeline(
        &self,
        input: &PipelineAnalysisInput,
        ctx: &AnalysisContext<S, D>,
    ) -> AnalyzerResult<AnalysisReport> {
        let mut state = AnalysisState::new(input.pipeline.name.clone());

        for stage in &self.stages {
            stage.run(input, ctx, &mut state).await?;
        }

        state.build()
    }
}
