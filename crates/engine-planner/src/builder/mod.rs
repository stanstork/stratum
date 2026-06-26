use crate::{
    builder::{
        analysis::{
            AnalysisContext, AnalysisContextConfig, AnalysisReport, AnalyzerError,
            AnalyzerRegistry, PipelineAnalysisInput,
        },
        analyzers::{connection::ConnectionAnalyzer, plugin::PluginAnalyzer, sample::SampleConfig},
        data_flow::DataFlowAnalyzer,
        diagnostics::diagnostic_generator::DiagnosticGenerator,
        endpoint::{is_wasm_pipeline, resolve_destination, resolve_source},
        errors::{ConnectionError, ReportBuilderError, ReportBuilderResult, SourceAnalyzerError},
        estimator::{DurationEstimator, ResourceEstimator},
        infra::{
            pipeline_analysis::{PipelineAnalysisResources, PipelineSettingsView},
            plan_metadata::MetadataGenerator,
        },
        summary::SummaryCalculator,
        utils::{MaskingPolicy, format_duration},
    },
    plan::{
        connection::{
            plan::ConnectionPlan,
            status::{ConnectionRole, ConnectionStatus},
        },
        define::{
            env_vars::{EnvVarUsage, ValueSource},
            resolved::{ResolvedConstant, ResolvedDefines},
        },
        diagnostics::{diagnostic::Diagnostic, level::DiagnosticLevel},
        error_handling::{
            failed_rows::{FailedRowsConfig, FailedRowsFormat},
            plan::{AfterMaxRetries, ErrorHandlingPlan},
            retry::{BackoffConfig, RetryConfig},
        },
        execution::{
            execution_settings::{ExecutionSettings, ExecutionStrategy, FailureStrategy},
            execution_stage::ExecutionStage,
            migration_report::MigrationReport,
        },
        pipeline::{plan::PipelinePlan, settings::PipelineSettings},
        sample::method::SamplingMethod,
    },
};
use connectors::traits::introspector::SchemaIntrospector;
use engine_config::settings::{
    Settings, validated::ValidatedSettings, validator::SettingsValidator,
};
use engine_core::{
    context::exec::ConnectionPool,
    dispatch_drivers,
    drivers::DriverRef,
    plan::execution::ExecutionPlan as CoreExecutionPlan,
    retry::RetryPolicy,
    schema::{
        plan::SchemaPlan,
        planner::SchemaPlanner,
        type_registry::{Dialect, TypeRegistry},
    },
};
use engine_processing::io::{destination::Destination, format::DataFormat, source::Source};
use engine_runtime::dag::Dag;
use engine_wasm::registry::{PluginRegistry, load_registry};
use model::execution::flags::IntegrityMode;
use model::execution::pipeline::RetryConfig as CoreRetryConfig;
use model::{
    core::value::Value,
    execution::{
        execution_config::FailureStrategy as CoreFailureStrategy,
        pipeline::{BackoffStrategy, ErrorHandling, FailedRowsDestination, FileFormat, Pipeline},
    },
};
use model::{
    execution::{
        define::DefinitionSource, execution_config::ExecutionStrategy as CoreExecutionStrategy,
    },
    transform::mapping::TransformationMetadata,
};
use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::Arc,
    time::Duration,
};
use tracing::info;

pub mod analysis;
pub mod analyzers;
pub mod data_flow;
pub mod diagnostics;
pub mod endpoint;
pub mod errors;
pub mod estimator;
pub mod explain;
pub mod infra;
pub mod plugin_validation;
pub mod summary;
pub mod utils;
pub mod wasm_schema;

/// Configuration for report building
pub struct ReportBuilderConfig {
    /// Enable sample data collection (--sample flag)
    pub enable_sampling: bool,

    /// Number of rows to sample per pipeline (--sample-size)
    pub sample_size: usize,

    /// Sampling method
    pub sample_method: SamplingMethod,

    /// Name of the ID column for sampling (--id-column)
    pub id_column: String,

    /// Specific IDs to sample (--sample-ids)
    pub sample_ids: Option<Vec<Value>>,

    /// Timeout for metadata queries
    pub metadata_timeout: Duration,

    /// Timeout for connection testing
    pub connection_timeout: Duration,

    /// Columns to mask in output
    pub mask_columns: Vec<String>,

    /// Auto-detect and mask sensitive columns
    pub auto_mask_sensitive: bool,

    /// Use exact COUNT for filtered rows (slower but accurate) vs EXPLAIN estimates (faster)
    pub exact_where: bool,

    /// Verbosity level (0 = quiet, 1 = normal, 2+ = verbose)
    pub verbosity: u8,
}

impl Default for ReportBuilderConfig {
    fn default() -> Self {
        Self {
            enable_sampling: false,
            sample_size: 5,
            sample_method: SamplingMethod::Random,
            id_column: "id".to_string(),
            sample_ids: None,
            metadata_timeout: Duration::from_secs(30),
            connection_timeout: Duration::from_secs(10),
            mask_columns: Vec::new(),
            auto_mask_sensitive: true,
            exact_where: false, // Use EXPLAIN by default (faster)
            verbosity: 1,
        }
    }
}

/// Main report builder that orchestrates all analysis
#[derive(Default)]
pub struct ReportBuilder {
    config: ReportBuilderConfig,
}

impl ReportBuilder {
    pub fn new(config: ReportBuilderConfig) -> Self {
        Self { config }
    }

    pub async fn build(
        &self,
        core_plan: &CoreExecutionPlan,
        dag: &Dag,
        config_path: &Path,
    ) -> Result<MigrationReport, ReportBuilderError> {
        info!("Orchestrating execution plan build for {:?}", config_path);

        // Preparation & Metadata
        let metadata = MetadataGenerator::generate(core_plan, config_path);
        let execution_settings = self.map_execution_settings(core_plan);
        let defines = self.resolve_defines(core_plan);

        // Connectivity
        let connections = self.collect_connections(core_plan).await?;
        let mut connection_pool = self.build_connection_pool(&connections, core_plan).await?;

        // Per-run plugin registry - shared with the executor so plan --sample
        // invokes WASM transforms identically to apply.
        let plugin_registry = load_registry(&core_plan.plugins)?;

        // Pipeline Analysis
        let mut pipelines = self
            .build_pipelines(core_plan, dag, &mut connection_pool, &plugin_registry)
            .await?;

        // Plan-time WASM plugin validation: type-checks transform/filter calls
        // against the source/destination column types now that both sides are analyzed.
        PluginAnalyzer::new().analyze(&mut pipelines, core_plan, &plugin_registry);

        // Post-Analysis Processing
        let execution_order = self.build_execution_stages(dag, &pipelines)?;
        let summary = SummaryCalculator::calculate(&pipelines, &connections);

        let diagnostics =
            DiagnosticGenerator::generate(&pipelines, &connections, &execution_settings);
        let estimations =
            ResourceEstimator::estimate(&pipelines, &execution_order, &execution_settings);
        let (is_executable, blocking_reason) = self.check_executability(&diagnostics, &pipelines);

        Ok(MigrationReport {
            plan_id: metadata.plan_id,
            generated_at: metadata.generated_at,
            engine_version: metadata.engine_version,
            config_hash: metadata.config_hash,
            config_path: metadata.config_path,
            execution_settings,
            defines,
            connections,
            pipelines,
            execution_order,
            summary,
            diagnostics,
            estimations,
            is_executable,
            blocking_reason,
        })
    }

    /// Iterates through core pipelines and runs the full analysis suite for each.
    async fn build_pipelines(
        &self,
        core_plan: &CoreExecutionPlan,
        dag: &Dag,
        connections: &mut ConnectionPool,
        plugin_registry: &Arc<PluginRegistry>,
    ) -> ReportBuilderResult<Vec<PipelinePlan>> {
        let mut pipelines = Vec::with_capacity(core_plan.pipelines.len());
        for pipeline in &core_plan.pipelines {
            pipelines.push(
                self.analyze_pipeline(pipeline, dag, connections, plugin_registry)
                    .await?,
            );
        }
        Ok(pipelines)
    }

    /// Conducts resource preparation and analytical reporting for a single pipeline.
    async fn analyze_pipeline(
        &self,
        pipeline: &Pipeline,
        dag: &Dag,
        connections: &mut ConnectionPool,
        plugin_registry: &Arc<PluginRegistry>,
    ) -> ReportBuilderResult<PipelinePlan> {
        info!("Analyzing pipeline: {}", pipeline.name);

        // WASM-aware path: when either endpoint is a WASM plugin the DB<->DB
        // analyzer chain can't run.
        if is_wasm_pipeline(pipeline) {
            return self
                .analyze_wasm_pipeline(pipeline, dag, connections, plugin_registry)
                .await;
        }

        // Prepare physical adapters and metadata caches via specialized resource container
        let resources =
            PipelineAnalysisResources::create(pipeline, connections, self, plugin_registry).await?;

        // Run specific analysis (schema validation, sampling, join analysis)
        let analysis_report = self
            .run_pipeline_analysis(pipeline, &resources, plugin_registry)
            .await
            .map_err(|e| {
                ReportBuilderError::SourceAnalyzer(SourceAnalyzerError::QueryFailed(format!(
                    "Analysis failed for {}: {}",
                    pipeline.name, e.message
                )))
            })?;

        // Final assembly of the plan
        self.assemble_pipeline_plan(pipeline, dag, resources, analysis_report)
            .await
    }

    /// Slim assembly path for pipelines with at least one WASM endpoint.
    async fn analyze_wasm_pipeline(
        &self,
        pipeline: &Pipeline,
        dag: &Dag,
        connections: &mut ConnectionPool,
        plugin_registry: &Arc<PluginRegistry>,
    ) -> ReportBuilderResult<PipelinePlan> {
        let src_ep = resolve_source(pipeline, connections, plugin_registry).await?;
        let dst_ep = resolve_destination(pipeline, connections, plugin_registry).await?;

        // A WASM source feeding a real DB destination can still create/extend the
        // destination table; preview those changes (mirrors the DB<->DB path).
        let schema_changes = wasm_schema::wasm_source_schema_changes(
            self,
            pipeline,
            src_ep.as_ref(),
            dst_ep.as_ref(),
            plugin_registry,
        )
        .await?;

        let (order, stage) = self
            .calculate_execution_positions(dag, &pipeline.name)
            .unwrap_or((0, 0));

        Ok(PipelinePlan {
            name: pipeline.name.clone(),
            description: pipeline.description.clone(),
            execution_order: order,
            execution_stage: stage,
            depends_on: dag.get_dependencies(&pipeline.name).unwrap_or_default(),
            source: src_ep.source_plan().clone(),
            destination: dst_ep.destination_plan().clone(),
            filters: Vec::new(),
            joins: Vec::new(),
            mappings: Vec::new(),
            validations: Vec::new(),
            error_handling: self.map_error_handling(&pipeline.error_handling),
            pagination: Default::default(),
            hooks: Default::default(),
            settings: Default::default(),
            data_flow_summary: Default::default(),
            schema_changes,
            diagnostics: Vec::new(),
            estimations: Default::default(),
            sample: None,
        })
    }

    /// Final step for a pipeline: calculates summaries, execution positions, and resource estimations.
    async fn assemble_pipeline_plan(
        &self,
        pipeline: &Pipeline,
        dag: &Dag,
        resources: PipelineAnalysisResources,
        report: analysis::AnalysisReport,
    ) -> ReportBuilderResult<PipelinePlan> {
        let settings = PipelineSettingsView::new(&resources.validated_settings);

        // Determine if we can use high-performance streaming (fast path)
        let is_fast_path = self
            .determine_fast_path(&resources, settings.create_missing_tables())
            .await;

        let settings = self.map_pipeline_settings(&resources.validated_settings);
        let estimations = DurationEstimator::new(is_fast_path).estimate_pipeline(
            &report.source,
            &report.destination,
            &report.mappings,
            &report.joins,
            &settings,
            is_fast_path,
        );

        let data_flow_summary = DataFlowAnalyzer::analyze(
            &report.mappings,
            &report.joins,
            &report.validations,
            &report.source,
            &settings,
        );
        let (order, stage) = self
            .calculate_execution_positions(dag, &pipeline.name)
            .unwrap_or((0, 0));

        let diagnostics = DiagnosticGenerator::for_pipeline(
            &pipeline.name,
            &report.source,
            &report.destination,
            &None,
            &report.joins,
            &report.mappings,
            &report.pagination,
        );

        Ok(PipelinePlan {
            name: pipeline.name.clone(),
            description: pipeline.description.clone(),
            execution_order: order,
            execution_stage: stage,
            depends_on: dag.get_dependencies(&pipeline.name).unwrap_or_default(),
            source: report.source,
            destination: report.destination,
            filters: report.filters.into_iter().collect(),
            joins: report.joins,
            mappings: report.mappings,
            validations: report.validations,
            error_handling: self.map_error_handling(&pipeline.error_handling),
            pagination: report.pagination.unwrap_or_default(),
            hooks: report.hooks,
            settings,
            data_flow_summary,
            schema_changes: report.schema_changes,
            diagnostics,
            estimations,
            sample: Some(report.sample),
        })
    }

    /// Orchestrates the actual analysis calls via the Registry.
    async fn run_pipeline_analysis(
        &self,
        pipeline: &Pipeline,
        resources: &PipelineAnalysisResources,
        plugin_registry: &Arc<PluginRegistry>,
    ) -> Result<AnalysisReport, AnalyzerError> {
        let config = &self.config;
        let ctx_config = AnalysisContextConfig {
            metadata_timeout: config.metadata_timeout,
            enable_sampling: config.enable_sampling,
            sample_size: config.sample_size,
            sample_method: config.sample_method.clone(),
            sample_ids: config.sample_ids.clone(),
            id_column: config.id_column.clone(),
            auto_mask_sensitive: config.auto_mask_sensitive,
            mask_columns: config.mask_columns.clone(),
            use_exact_where: config.exact_where,
        };

        let analysis_input = PipelineAnalysisInput::new(
            Arc::new(pipeline.clone()),
            self.sample_config(),
            PipelineSettingsView::new(&resources.validated_settings).mapped_columns_only(),
        );

        let schema_plan = resources.schema_plan.clone();
        let mapping = resources.mapping.clone();
        let timeout = config.metadata_timeout;

        dispatch_drivers!(&resources.src_driver, &resources.dst_driver, |src, dst| {
            let analysis_context = AnalysisContext::new(
                src.clone(),
                resources.src_driver.dialect(),
                dst.clone(),
                resources.dst_driver.dialect(),
                schema_plan.clone(),
                Arc::new(mapping.clone()),
                plugin_registry.clone(),
                ctx_config,
            );

            let registry = AnalyzerRegistry::new(
                analysis_context.source_cache.clone(),
                schema_plan.clone(),
                &mapping,
                dst.clone(),
                timeout,
            );

            registry
                .analyze_pipeline(&analysis_input, &analysis_context)
                .await
        })
    }

    async fn collect_connections(
        &self,
        core_plan: &CoreExecutionPlan,
    ) -> ReportBuilderResult<Vec<ConnectionPlan>> {
        // Determine connection roles based on pipeline usage
        let roles = Self::determine_connection_roles(core_plan);

        let analyzer = ConnectionAnalyzer::new(self.config.connection_timeout);
        let mut results = Vec::new();
        for core_conn in &core_plan.connections {
            let mut plan = analyzer.analyze(core_conn).await.map_err(|e| {
                ReportBuilderError::Connection(ConnectionError::Failed {
                    name: core_conn.name.clone(),
                    reason: e.to_string(),
                })
            })?;

            // Update role based on actual usage
            if let Some(role) = roles.get(&core_conn.name) {
                plan.role = role.clone();
            }

            results.push(plan);
        }
        Ok(results)
    }

    /// Determine connection roles by analyzing which pipelines use them as source vs destination
    fn determine_connection_roles(
        core_plan: &CoreExecutionPlan,
    ) -> std::collections::HashMap<String, ConnectionRole> {
        let mut source_connections = HashSet::new();
        let mut dest_connections = HashSet::new();

        for pipeline in &core_plan.pipelines {
            source_connections.insert(pipeline.source.connection.name.clone());
            dest_connections.insert(pipeline.destination.connection.name.clone());
        }

        let mut roles = HashMap::new();
        for conn in &core_plan.connections {
            let is_source = source_connections.contains(&conn.name);
            let is_dest = dest_connections.contains(&conn.name);

            let role = match (is_source, is_dest) {
                (true, true) => ConnectionRole::Both,
                (true, false) => ConnectionRole::Source,
                (false, true) => ConnectionRole::Destination,
                (false, false) => ConnectionRole::Both, // Unused, default to Both
            };
            roles.insert(conn.name.clone(), role);
        }

        roles
    }

    async fn build_connection_pool(
        &self,
        connection_plans: &[ConnectionPlan],
        core_plan: &CoreExecutionPlan,
    ) -> ReportBuilderResult<ConnectionPool> {
        let mut pool = ConnectionPool::new();
        for (plan, core_conn) in connection_plans.iter().zip(&core_plan.connections) {
            if let ConnectionStatus::Connected { .. } = &plan.status {
                pool.get_or_create(core_conn).await.map_err(|e| {
                    ReportBuilderError::Connection(ConnectionError::Failed {
                        name: core_conn.name.clone(),
                        reason: e.to_string(),
                    })
                })?;
            }
        }
        Ok(pool)
    }

    fn build_execution_stages(
        &self,
        dag: &Dag,
        pipelines: &[PipelinePlan],
    ) -> ReportBuilderResult<Vec<ExecutionStage>> {
        let stages = dag.execution_order();
        Ok(stages
            .iter()
            .enumerate()
            .map(|(idx, pipeline_names)| {
                let stage_pipelines: Vec<_> = pipelines
                    .iter()
                    .filter(|p| pipeline_names.contains(&p.name))
                    .collect();
                ExecutionStage {
                    stage: idx,
                    pipelines: pipeline_names.to_vec(),
                    estimated_duration: DurationEstimator::estimate_stage(&stage_pipelines),
                }
            })
            .collect())
    }

    fn calculate_execution_positions(
        &self,
        dag: &Dag,
        pipeline_name: &str,
    ) -> Option<(usize, usize)> {
        let execution_order = dag.execution_order();
        let mut cumulative_order = 0;

        for (stage_idx, stage_pipelines) in execution_order.iter().enumerate() {
            if let Some(pos) = stage_pipelines.iter().position(|p| p == pipeline_name) {
                return Some((cumulative_order + pos, stage_idx));
            }
            cumulative_order += stage_pipelines.len();
        }
        None
    }

    pub(crate) async fn validate_settings(
        &self,
        pipeline: &Pipeline,
        source: &Source,
        dest: &Destination,
        introspector: &dyn SchemaIntrospector,
    ) -> ReportBuilderResult<ValidatedSettings> {
        let settings = Settings::from_map(&pipeline.settings);
        let validator =
            SettingsValidator::new(source, dest, introspector, true, IntegrityMode::Off);
        validator.validate(&settings).await.map_err(|e| {
            ReportBuilderError::Config(format!("Validation failed for {}: {}", pipeline.name, e))
        })
    }

    pub(crate) async fn build_schema_plan(
        &self,
        pipeline: &Pipeline,
        introspector: Arc<dyn SchemaIntrospector>,
        source_dialect: Dialect,
        mapping: &TransformationMetadata,
        settings: &ValidatedSettings,
    ) -> ReportBuilderResult<SchemaPlan> {
        let view = PipelineSettingsView::new(settings);
        let target_dialect = DataFormat::parse(&pipeline.destination.connection.driver)
            .map(|f| f.to_dialect())
            .unwrap_or(Dialect::Postgres);
        let type_registry = TypeRegistry::new(source_dialect, target_dialect);
        let planner = SchemaPlanner::new(
            introspector.clone(),
            source_dialect,
            mapping.clone(),
            view.ignore_constraints(),
            view.mapped_columns_only(),
            type_registry,
        );

        let join_tables: Vec<&str> = pipeline
            .source
            .joins
            .iter()
            .map(|j| j.table.as_str())
            .collect();
        let mut plan = planner.plan_schema(&pipeline.source.table).await?;
        for &join_table in join_tables.iter() {
            let meta = introspector.table_metadata(join_table).await?;
            plan.add_metadata(join_table, meta);
        }

        Ok(plan)
    }

    fn check_executability(
        &self,
        diagnostics: &[Diagnostic],
        pipelines: &[PipelinePlan],
    ) -> (bool, Option<String>) {
        let top_level_errors = diagnostics
            .iter()
            .filter(|d| d.level == DiagnosticLevel::Error);
        let pipeline_errors = pipelines
            .iter()
            .flat_map(|p| p.diagnostics.iter())
            .filter(|d| d.level == DiagnosticLevel::Error);
        let errors: Vec<_> = top_level_errors
            .chain(pipeline_errors)
            .map(|e| e.message.clone())
            .collect();
        if errors.is_empty() {
            (true, None)
        } else {
            (false, Some(errors.join("; ")))
        }
    }

    fn map_execution_settings(&self, core: &CoreExecutionPlan) -> ExecutionSettings {
        ExecutionSettings {
            strategy: match core.execution_config.strategy {
                CoreExecutionStrategy::Sequential => ExecutionStrategy::Sequential,
                CoreExecutionStrategy::Parallel => ExecutionStrategy::Parallel,
            },
            max_concurrency: core.execution_config.max_concurrency.unwrap_or(1) as usize,
            on_failure: match core.execution_config.on_failure {
                CoreFailureStrategy::FailFast => FailureStrategy::FailFast,
                CoreFailureStrategy::Continue => FailureStrategy::Continue,
            },
        }
    }

    fn map_error_handling(&self, core: &Option<ErrorHandling>) -> ErrorHandlingPlan {
        core.as_ref().map_or(ErrorHandlingPlan::default(), |eh| {
            let retry_policy = RetryPolicy::from_config(eh.retry.as_ref());

            ErrorHandlingPlan {
                retry: Some(RetryConfig {
                    max_attempts: retry_policy.max_attempts,
                    backoff: self.map_backoff(&eh.retry, &retry_policy),
                }),
                failed_rows: self.map_failed_rows(eh),
                after_max_retries: AfterMaxRetries::Fail,
            }
        })
    }

    fn map_backoff(&self, r: &Option<CoreRetryConfig>, p: &RetryPolicy) -> BackoffConfig {
        if let Some(r) = r {
            let delay = Duration::from_millis(r.delay_ms);
            match r.backoff {
                BackoffStrategy::Fixed => BackoffConfig::Fixed {
                    delay: format_duration(&delay),
                },
                BackoffStrategy::Exponential => BackoffConfig::Exponential {
                    initial_delay: format_duration(&delay),
                    max_delay: Some(format_duration(&Duration::from_secs(5))),
                },
                BackoffStrategy::Linear => BackoffConfig::Linear {
                    delay: format_duration(&Duration::from_millis(
                        r.delay_ms * r.max_attempts as u64,
                    )),
                },
            }
        } else {
            BackoffConfig::Exponential {
                initial_delay: format_duration(&p.base_delay),
                max_delay: Some(format_duration(&p.max_delay)),
            }
        }
    }

    fn map_failed_rows(&self, eh: &ErrorHandling) -> Option<FailedRowsConfig> {
        eh.failed_rows
            .as_ref()?
            .destination
            .as_ref()
            .map(|dest| match dest {
                FailedRowsDestination::Table {
                    connection,
                    table,
                    schema,
                } => FailedRowsConfig::Table {
                    connection: connection.name.clone(),
                    table: table.clone(),
                    schema: schema.clone(),
                },
                FailedRowsDestination::File { path, format } => FailedRowsConfig::File {
                    path: path.clone(),
                    format: match format {
                        FileFormat::Json => FailedRowsFormat::Jsonl,
                        FileFormat::Csv => FailedRowsFormat::Csv,
                        FileFormat::Parquet => FailedRowsFormat::Parquet,
                    },
                },
            })
    }

    fn map_pipeline_settings(&self, validated: &ValidatedSettings) -> PipelineSettings {
        PipelineSettings::from_validated(validated.clone())
    }

    fn resolve_defines(&self, core: &CoreExecutionPlan) -> ResolvedDefines {
        let constants = core
            .definitions
            .variables
            .iter()
            .map(|(name, def)| ResolvedConstant {
                name: name.clone(),
                value: Self::mask_value(&def.value),
                source: match &def.source {
                    DefinitionSource::Literal => ValueSource::Literal,
                    DefinitionSource::Environment { var_name } => ValueSource::Environment {
                        var_name: var_name.clone(),
                    },
                    DefinitionSource::EnvironmentWithDefault {
                        var_name,
                        default_value,
                    } => ValueSource::EnvironmentWithDefault {
                        var_name: var_name.clone(),
                        default: MaskingPolicy::mask_url(default_value),
                    },
                },
            })
            .collect();

        let masking = MaskingPolicy::new(
            self.config.auto_mask_sensitive,
            self.config.mask_columns.clone(),
        );
        let env_vars_used = core
            .env_vars
            .iter()
            .map(|(name, var)| EnvVarUsage {
                var_name: name.clone(),
                was_set: var.was_set,
                used_default: var.used_default,
                value: masking.mask_env_var_value(name, &var.value),
            })
            .collect();

        ResolvedDefines {
            constants,
            env_vars_used,
        }
    }

    fn mask_value(value: &Value) -> String {
        if let Some(s) = value.as_string() {
            if MaskingPolicy::is_db_url(&s) {
                return MaskingPolicy::mask_url(&s);
            }
            return s;
        }
        format!("{:?}", value)
    }

    fn sample_config(&self) -> SampleConfig {
        SampleConfig {
            enabled: self.config.enable_sampling,
            size: self.config.sample_size,
            method: self.config.sample_method.clone(),
            mask_columns: self.config.mask_columns.clone(),
            auto_mask_sensitive: self.config.auto_mask_sensitive,
            sample_ids: self.config.sample_ids.clone(),
            id_column: self.config.id_column.clone(),
        }
    }

    async fn determine_fast_path(
        &self,
        resources: &PipelineAnalysisResources,
        create_missing: bool,
    ) -> bool {
        let sink = resources.core_data_destination.sink();
        match sink.support_fast_path().await {
            Ok(true) => {
                // Check if destination table has primary keys
                match resources
                    .dst_driver
                    .table_metadata(&resources.core_data_destination.name())
                    .await
                {
                    Ok(meta) => !meta.primary_keys.is_empty(),
                    Err(_) if create_missing => {
                        // Check source table for primary keys
                        let src_table = resources
                            .mapping
                            .entities
                            .reverse_resolve(&resources.core_data_destination.name());
                        match resources.src_driver.table_metadata(&src_table).await {
                            Ok(meta) => !meta.primary_keys.is_empty(),
                            Err(_) => false,
                        }
                    }
                    Err(_) => false,
                }
            }
            _ => false,
        }
    }
}
