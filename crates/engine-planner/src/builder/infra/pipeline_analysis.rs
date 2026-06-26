use crate::builder::{
    ReportBuilder, errors::ReportBuilderResult, infra::metadata_cache::MetadataCacheRef,
};
use connectors::traits::introspector::SchemaIntrospector;
use engine_config::settings::validated::ValidatedSettings;
use engine_core::{
    context::exec::ConnectionPool, dispatch_driver, drivers::DriverRef, schema::plan::SchemaPlan,
};
use engine_processing::io::{
    destination::{Destination, IntoDestination},
    source::Source,
};
use engine_wasm::registry::{PluginRegistry, plugin_columns};
use model::{execution::pipeline::Pipeline, transform::mapping::TransformationMetadata};
use query_builder::offsets::OffsetStrategyFactory;
use std::sync::Arc;

/// Encapsulates the physical and logical resources required to analyze a single pipeline.
/// No longer generic - uses DriverRef enum pattern instead.
pub struct PipelineAnalysisResources {
    pub src_driver: DriverRef,
    pub dst_driver: DriverRef,
    pub source_cache: MetadataCacheRef,
    pub dest_cache: MetadataCacheRef,
    pub mapping: TransformationMetadata,
    pub core_data_source: Arc<Source>,
    pub core_data_destination: Destination,
    pub schema_plan: Arc<SchemaPlan>,
    pub validated_settings: ValidatedSettings,
}

impl PipelineAnalysisResources {
    /// Create resources from ConnectionPool by matching on driver types.
    pub async fn create(
        pipeline: &Pipeline,
        connections: &mut ConnectionPool,
        builder: &ReportBuilder,
        plugin_registry: &Arc<PluginRegistry>,
    ) -> ReportBuilderResult<Self> {
        let src_driver = DriverRef::resolve(
            &pipeline.source.connection.driver,
            &pipeline.source.connection,
            connections,
        )
        .await?;

        let dst_driver = DriverRef::resolve(
            &pipeline.destination.connection.driver,
            &pipeline.destination.connection,
            connections,
        )
        .await?;

        Self::build(pipeline, src_driver, dst_driver, builder, plugin_registry).await
    }

    /// Build all analysis resources from resolved drivers.
    async fn build(
        pipeline: &Pipeline,
        src_driver: DriverRef,
        dst_driver: DriverRef,
        builder: &ReportBuilder,
        plugin_registry: &Arc<PluginRegistry>,
    ) -> ReportBuilderResult<Self> {
        let source_cache = MetadataCacheRef::new(
            &src_driver,
            src_driver.dialect(),
            builder.config.metadata_timeout,
        );
        let dest_cache = MetadataCacheRef::new(
            &dst_driver,
            dst_driver.dialect(),
            builder.config.metadata_timeout,
        );

        let mut mapping = TransformationMetadata::new(pipeline);
        mapping.set_plugin_columns(plugin_columns(pipeline, plugin_registry));

        let offset_strategy = OffsetStrategyFactory::from_pagination(&pipeline.source.pagination);

        let core_data_source = dispatch_driver!(&src_driver, |d| {
            Arc::new(Source::new(d.clone(), pipeline, &mapping, offset_strategy).await?)
        });

        let source_dialect = src_driver.dialect();
        let core_data_destination = dispatch_driver!(dst_driver.clone(), |d| {
            d.clone()
                .into_destination(&pipeline.destination.table, source_dialect)
        });

        let introspector = dispatch_driver!(&src_driver, |d| {
            d.clone() as Arc<dyn SchemaIntrospector>
        });

        let validated_settings = builder
            .validate_settings(
                pipeline,
                &core_data_source,
                &core_data_destination,
                introspector.as_ref(),
            )
            .await?;

        let schema_plan = Arc::new(
            builder
                .build_schema_plan(
                    pipeline,
                    introspector.clone(),
                    source_dialect,
                    &mapping,
                    &validated_settings,
                )
                .await?,
        );

        Ok(Self {
            src_driver,
            dst_driver,
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
