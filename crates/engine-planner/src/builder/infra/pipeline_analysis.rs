use crate::builder::{
    ReportBuilder, errors::ReportBuilderResult, infra::metadata_cache::MetadataCacheRef,
};
use engine_config::settings::validated::ValidatedSettings;
use engine_core::{
    context::exec::ConnectionPool,
    dispatch_driver,
    drivers::DriverRef,
    schema::{plan::SchemaPlan, type_registry::Dialect},
};
use engine_processing::io::{destination::Destination, source::Source};
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

        Self::build(pipeline, src_driver, dst_driver, builder).await
    }

    /// Build all analysis resources from resolved drivers.
    async fn build(
        pipeline: &Pipeline,
        src_driver: DriverRef,
        dst_driver: DriverRef,
        builder: &ReportBuilder,
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

        let mapping = TransformationMetadata::new(pipeline);
        let offset_strategy = OffsetStrategyFactory::from_pagination(&pipeline.source.pagination);

        let core_data_source = dispatch_driver!(&src_driver, |d| {
            Arc::new(Source::new(d.clone(), pipeline, &mapping, offset_strategy).await?)
        });

        let source_dialect = src_driver.dialect();

        let core_data_destination =
            Self::create_destination(&dst_driver, pipeline, source_dialect)?;

        let introspector: Arc<dyn connectors::traits::introspector::SchemaIntrospector> =
            match &src_driver {
                DriverRef::Postgres(d) => d.clone(),
                DriverRef::MySql(d) => d.clone(),
            };

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

    /// Create a destination sink. Unsupported drivers return an error.
    fn create_destination(
        driver: &DriverRef,
        pipeline: &Pipeline,
        source_dialect: Dialect,
    ) -> ReportBuilderResult<Destination> {
        match driver {
            DriverRef::Postgres(d) => Ok(Destination::postgres(
                d.clone(),
                &pipeline.destination.table,
                source_dialect,
            )),
            // Uncomment as destination support is implemented:
            // DriverRef::MySql(d) => Ok(Destination::mysql(
            //     d.clone(),
            //     &pipeline.destination.table,
            //     source_dialect,
            // )),
            _ => Err(
                crate::builder::errors::ReportBuilderError::UnsupportedDriver(format!(
                    "{:?} destination not yet supported in planner for pipeline '{}'",
                    driver.dialect(),
                    pipeline.name
                )),
            ),
        }
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
