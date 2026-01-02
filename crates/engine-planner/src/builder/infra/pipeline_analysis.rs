use crate::builder::{
    PlanBuilder, errors::PlanBuilderResult, infra::metadata_cache::MetadataCache,
};
use connectors::adapter::Adapter;
use engine_config::settings::validated::ValidatedSettings;
use engine_core::{
    connectors::{destination::Destination, source::Source},
    context::exec::ConnectionPool,
    schema::plan::SchemaPlan,
};
use model::{execution::pipeline::Pipeline, transform::mapping::TransformationMetadata};
use query_builder::offsets::OffsetStrategyFactory;
use std::sync::Arc;

/// Encapsulates the physical and logical resources required to analyze a single pipeline.
pub struct PipelineAnalysisResources {
    pub source_adapter: Arc<Adapter>,
    pub dest_adapter: Arc<Adapter>,
    pub source_cache: Arc<MetadataCache>,
    pub dest_cache: Arc<MetadataCache>,
    pub mapping: TransformationMetadata,
    pub core_data_source: Arc<Source>,
    pub core_data_destination: Destination,
    pub schema_plan: Arc<SchemaPlan>,
    pub validated_settings: ValidatedSettings,
}

impl PipelineAnalysisResources {
    /// Factory method to initialize all necessary resources for a pipeline.
    pub async fn create(
        pipeline: &Pipeline,
        connections: &mut ConnectionPool,
        builder: &PlanBuilder,
    ) -> PlanBuilderResult<Self> {
        let source_adapter = Arc::new(
            connections
                .get_or_create(&pipeline.source.connection)
                .await?,
        );
        let dest_adapter = Arc::new(
            connections
                .get_or_create(&pipeline.destination.connection)
                .await?,
        );

        let source_cache = Arc::new(MetadataCache::new(
            source_adapter.clone(),
            builder.config.metadata_timeout,
        ));
        let dest_cache = Arc::new(MetadataCache::new(
            dest_adapter.clone(),
            builder.config.metadata_timeout,
        ));

        let mapping = TransformationMetadata::new(pipeline);
        let offset_strategy = OffsetStrategyFactory::from_pagination(&pipeline.source.pagination);

        let core_data_source = Arc::new(
            Source::new(
                source_adapter.as_ref().clone(),
                pipeline,
                &mapping,
                offset_strategy,
            )
            .await?,
        );
        let core_data_destination = Destination::new(
            dest_adapter.as_ref().clone(),
            &pipeline.destination.table,
            &pipeline.destination.connection,
        )
        .await?;

        let validated_settings = builder
            .validate_settings(pipeline, &core_data_source, &core_data_destination)
            .await?;
        let schema_plan = Arc::new(
            builder
                .build_schema_plan(pipeline, &core_data_source, &mapping, &validated_settings)
                .await?,
        );

        Ok(Self {
            source_adapter,
            dest_adapter,
            source_cache,
            dest_cache,
            mapping,
            core_data_source,
            core_data_destination,
            schema_plan,
            validated_settings,
        })
    }
}

pub struct PipelineSettingsView<'a> {
    settings: &'a ValidatedSettings,
}

impl<'a> PipelineSettingsView<'a> {
    pub fn new(settings: &'a ValidatedSettings) -> Self {
        Self { settings }
    }

    pub fn ignore_constraints(&self) -> bool {
        self.settings.ignore_constraints()
    }

    pub fn mapped_columns_only(&self) -> bool {
        self.settings.mapped_columns_only()
    }

    pub fn batch_size(&self) -> usize {
        self.settings.batch_size()
    }

    pub fn create_missing_tables(&self) -> bool {
        self.settings.create_missing_tables()
    }
}
