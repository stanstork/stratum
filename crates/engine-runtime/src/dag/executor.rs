use crate::{dag::Dag, error::MigrationError, execution::orchestrator::PipelineOrchestrator};
use connectors::{sql::metadata::table::TableMetadata, traits::introspector::SchemaIntrospector};
use engine_config::settings::{self, ValidatedSettings};
use engine_core::{
    context::{env::EnvContext, exec::ExecutionContext},
    dispatch_driver, dispatch_drivers,
    drivers::DriverRef,
    event_bus::bus::EventBus,
    plan::{cascade::resolve_cascade_tables, execution::ExecutionPlan},
    schema::{
        graph_expander::GraphExpander,
        schema_ops::SchemaOps,
        type_registry::{Dialect, TypeRegistry},
    },
    state::{StateStore, models::WalEntry, sled_store::SledStateStore},
    utils::make_item_id,
};
use engine_infra::shutdown::ShutdownSignal;
use engine_processing::{
    context::PipelineContext,
    io::{
        destination::{Destination, IntoDestination},
        source::Source,
    },
};
use engine_state::models::{PauseReason, PipelineRunState, PipelineStatus, RunState, RunStatus};
use futures::stream::{self, StreamExt};
use model::{
    events::migration::MigrationEvent,
    execution::{
        execution_config::{ExecutionConfig, ExecutionStrategy, FailureStrategy},
        flags::ExecutionFlags,
        pipeline::Pipeline,
        references::{DataMode, GraphReferences},
    },
};
use model::{pagination::cursor::Cursor, transform::mapping::TransformationMetadata};
use query_builder::offsets::{OffsetStrategy, OffsetStrategyFactory};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

pub struct DagExecutor {
    plan: ExecutionPlan,
    flags: ExecutionFlags,
    shutdown: ShutdownSignal,
    exec_ctx: ExecutionContext,
    exec_config: ExecutionConfig,
    event_bus: EventBus,
    done_ops: Arc<Mutex<HashSet<String>>>,
}

impl DagExecutor {
    /// Create executor without event bus (headless mode)
    pub async fn new(
        plan: ExecutionPlan,
        flags: ExecutionFlags,
        shutdown: ShutdownSignal,
        env: Arc<EnvContext>,
    ) -> Result<Self, MigrationError> {
        let event_bus = EventBus::new();
        Self::subscribe_to_events(&event_bus).await;
        Self::init(plan, flags, shutdown, event_bus, env).await
    }

    /// Create executor with event bus for external monitoring (TUI/Pretty mode)
    pub async fn with_event_bus(
        plan: ExecutionPlan,
        flags: ExecutionFlags,
        shutdown: ShutdownSignal,
        event_bus: EventBus,
        env: Arc<EnvContext>,
    ) -> Result<Self, MigrationError> {
        Self::init(plan, flags, shutdown, event_bus, env).await
    }

    async fn init(
        plan: ExecutionPlan,
        flags: ExecutionFlags,
        shutdown: ShutdownSignal,
        event_bus: EventBus,
        env: Arc<EnvContext>,
    ) -> Result<Self, MigrationError> {
        let home_dir = dirs::home_dir().ok_or_else(|| {
            MigrationError::InitializationError("Could not determine home directory".to_string())
        })?;

        let state = Arc::new(
            SledStateStore::open(home_dir.join(".stratum/state")).map_err(|e| {
                MigrationError::InitializationError(format!("Failed to open state store: {e}"))
            })?,
        );
        let exec_ctx = ExecutionContext::new(&plan, state, env).await?;
        let exec_config = plan.execution_config.clone();

        Ok(Self {
            plan,
            flags,
            shutdown,
            exec_ctx,
            exec_config,
            event_bus,
            done_ops: Arc::new(Mutex::new(HashSet::new())),
        })
    }

    async fn subscribe_to_events(event_bus: &EventBus) {
        let (event_tx, mut event_rx) = mpsc::channel::<Arc<MigrationEvent>>(100);
        event_bus.subscribe::<MigrationEvent>(event_tx).await;

        tokio::spawn(async move {
            while let Some(event) = event_rx.recv().await {
                info!("Migration Event: {}", event);
            }
        });

        info!("Event subscriber configured for migration monitoring");
    }

    pub async fn execute(self, dag: Dag) -> Result<(), MigrationError> {
        let mut failed_pipelines = HashSet::new();

        // Initialize state or resume from a paused run
        let (mut run_state, mut completed_pipelines) = self.init_or_resume_run().await?;

        // Execute levels, updating run_state as pipelines complete
        let run_result = self
            .execute_levels(
                &dag,
                &mut run_state,
                &mut completed_pipelines,
                &mut failed_pipelines,
            )
            .await;

        // Complete run and finalize state
        self.finalize_run(run_result, run_state, failed_pipelines)
            .await
    }

    async fn init_or_resume_run(&self) -> Result<(RunState, HashSet<String>), MigrationError> {
        let run_id = self.exec_ctx.run_id();
        let existing_run = self.exec_ctx.state.load_run_state(&run_id).await?;

        let resuming = matches!(
            existing_run.as_ref().map(|r| &r.status),
            Some(RunStatus::Paused { .. })
        );

        let completed_pipelines: HashSet<String> = existing_run
            .as_ref()
            .map(|r| {
                r.pipelines
                    .iter()
                    .filter(|p| p.status == PipelineStatus::Completed)
                    .map(|p| p.name.clone())
                    .collect()
            })
            .unwrap_or_default();

        if resuming {
            info!(
                "Resuming paused migration {} ({} pipelines already completed)",
                run_id,
                completed_pipelines.len()
            );
        }

        let run_state = self.build_initial_run_state(&run_id, existing_run, &completed_pipelines);

        self.exec_ctx.state.save_run_state(&run_state).await?;

        let wal_entry = if resuming {
            WalEntry::RunResumed { run_id }
        } else {
            WalEntry::RunStart {
                run_id,
                plan_hash: self.plan.hash().to_string(),
            }
        };
        self.exec_ctx.state.append_wal(&wal_entry).await?;

        Ok((run_state, completed_pipelines))
    }

    fn build_initial_run_state(
        &self,
        run_id: &str,
        existing_run: Option<RunState>,
        completed_pipelines: &HashSet<String>,
    ) -> RunState {
        let pipelines = self
            .plan
            .pipelines
            .iter()
            .enumerate()
            .map(|(idx, p)| {
                let item_id = make_item_id(self.plan.hash(), &p.destination.table, idx);

                if completed_pipelines.contains(&p.name) {
                    let rows_done = existing_run
                        .as_ref()
                        .and_then(|r| r.pipelines.iter().find(|ps| ps.name == p.name))
                        .map(|ps| ps.rows_done)
                        .unwrap_or(0);

                    PipelineRunState {
                        name: p.name.clone(),
                        item_id,
                        status: PipelineStatus::Completed,
                        rows_done,
                        total_rows: None,
                    }
                } else {
                    let rows_done = existing_run
                        .as_ref()
                        .and_then(|r| r.pipelines.iter().find(|ps| ps.name == p.name))
                        .map(|ps| ps.rows_done)
                        .unwrap_or(0);

                    PipelineRunState {
                        name: p.name.clone(),
                        item_id,
                        status: PipelineStatus::Pending,
                        rows_done,
                        total_rows: None,
                    }
                }
            })
            .collect();

        RunState {
            run_id: run_id.to_string(),
            config_path: self.plan.config_path.clone(),
            config_hash: self.plan.hash().to_string(),
            status: RunStatus::Running,
            started_at: existing_run
                .map(|r| r.started_at)
                .unwrap_or_else(chrono::Utc::now),
            total_pipelines: self.plan.pipelines.len(),
            pipelines,
        }
    }

    async fn execute_levels(
        &self,
        dag: &Dag,
        run_state: &mut RunState,
        completed_pipelines: &mut HashSet<String>,
        failed_pipelines: &mut HashSet<String>,
    ) -> Result<(), MigrationError> {
        let execution_order = dag.execution_order();
        info!(
            "Executing {} pipelines in {} levels",
            dag.total_pipelines(),
            execution_order.len()
        );

        for (level_idx, level) in execution_order.iter().enumerate() {
            let level_remaining: Vec<String> = level
                .iter()
                .filter(|name| !completed_pipelines.contains(*name))
                .cloned()
                .collect();

            let executable = self.filter_executable(&level_remaining, failed_pipelines);

            if executable.is_empty() {
                if level_remaining.is_empty() {
                    info!("Level {}: all pipelines already completed", level_idx + 1);
                } else {
                    warn!("All pipelines in level {} skipped", level_idx + 1);
                }
                continue;
            }

            info!(
                "Level {}/{}: Executing {} pipelines (skipped {}): {:?}",
                level_idx + 1,
                execution_order.len(),
                executable.len(),
                level.len() - executable.len(),
                executable
            );

            // Check for process cancellation
            if self.shutdown.cancel.is_cancelled() {
                warn!("Shutdown requested");
                return Err(MigrationError::ShutdownRequested);
            }

            // Check for requested pauses
            if self.shutdown.pause.is_cancelled() {
                info!("Pause requested - saving state");
                self.save_paused_state(
                    run_state,
                    failed_pipelines,
                    completed_pipelines,
                    PauseReason::Manual,
                )
                .await?;
                return Err(MigrationError::Paused);
            }

            // Execute the current level's pipelines
            match self
                .execute_level(
                    &executable,
                    run_state,
                    completed_pipelines,
                    failed_pipelines,
                )
                .await
            {
                Err(MigrationError::Paused) => {
                    info!(
                        "Pause detected during level {} - saving state",
                        level_idx + 1
                    );

                    self.save_paused_state(
                        run_state,
                        failed_pipelines,
                        completed_pipelines,
                        PauseReason::Manual,
                    )
                    .await?;

                    return Err(MigrationError::Paused);
                }
                Err(e) => return Err(e),
                Ok(()) => {}
            }
        }

        Ok(())
    }

    async fn finalize_run(
        self,
        run_result: Result<(), MigrationError>,
        mut run_state: RunState,
        failed_pipelines: HashSet<String>,
    ) -> Result<(), MigrationError> {
        match run_result {
            // Process finalize state when the migration actually completed cleanly or handled its pipeline failures
            Ok(()) | Err(MigrationError::PipelinesFailed(_)) => {
                run_state.status = RunStatus::Completed {
                    completed_at: chrono::Utc::now(),
                };

                let run_id = self.exec_ctx.run_id();
                for ps in &mut run_state.pipelines {
                    if ps.status == PipelineStatus::Pending {
                        if failed_pipelines.contains(&ps.name) {
                            ps.status = PipelineStatus::Failed {
                                error: "Failed during execution".to_string(),
                            };
                        } else {
                            ps.status = PipelineStatus::Completed;
                        }
                    }

                    // Always refresh rows_done from checkpoint (authoritative cumulative count)
                    if let Ok(Some(cp)) = self
                        .exec_ctx
                        .state
                        .load_checkpoint(&run_id, &ps.item_id, "part-0")
                        .await
                    {
                        ps.rows_done = cp.rows_done;
                    }
                }

                self.exec_ctx.state.save_run_state(&run_state).await?;
                self.exec_ctx
                    .state
                    .append_wal(&WalEntry::RunDone {
                        run_id: self.exec_ctx.run_id(),
                    })
                    .await?;

                self.finalize(failed_pipelines)
            }
            // Retain unhandled structural errors (Pauses, Manual Cancelations) directly
            Err(e) => Err(e),
        }
    }

    fn finalize(self, failed_pipelines: HashSet<String>) -> Result<(), MigrationError> {
        if failed_pipelines.is_empty() {
            info!("Migration completed successfully with all pipelines");
            return Ok(());
        }

        let failed_list: Vec<String> = failed_pipelines.into_iter().collect();
        error!(
            "Migration completed with {} failed/skipped pipelines: {:?}",
            failed_list.len(),
            failed_list
        );

        match self.exec_config.on_failure {
            FailureStrategy::Continue => {
                warn!(
                    "Continue strategy: returning Ok despite {} failed/skipped pipelines",
                    failed_list.len()
                );
                Ok(())
            }
            FailureStrategy::FailFast => Err(MigrationError::PipelinesFailed(failed_list)),
        }
    }

    /// Filter pipelines in a level, marking skipped ones as failed so dependents propagate.
    fn filter_executable(
        &self,
        level: &[String],
        failed_pipelines: &mut HashSet<String>,
    ) -> Vec<String> {
        let mut executable = Vec::new();

        for name in level {
            let pipeline = self
                .plan
                .pipelines
                .iter()
                .find(|p| &p.name == name)
                .unwrap();

            let has_failed_dep = pipeline
                .dependencies
                .iter()
                .any(|dep| failed_pipelines.contains(dep));

            if has_failed_dep {
                warn!("Skipping pipeline '{}' due to failed dependency", name);
                failed_pipelines.insert(name.clone());
            } else {
                executable.push(name.clone());
            }
        }

        executable
    }

    /// Execute a level of pipelines according to the configured strategy.
    async fn execute_level(
        &self,
        executable: &[String],
        run_state: &mut RunState,
        completed_pipelines: &mut HashSet<String>,
        failed_pipelines: &mut HashSet<String>,
    ) -> Result<(), MigrationError> {
        let results: Vec<(String, Result<u64, MigrationError>)> = match self.exec_config.strategy {
            ExecutionStrategy::Sequential => {
                let mut results = Vec::new();
                for name in executable {
                    let result = self.execute_pipeline(name).await;
                    let should_fail_fast = result.is_err()
                        && matches!(self.exec_config.on_failure, FailureStrategy::FailFast);
                    results.push((name.clone(), result));
                    if should_fail_fast {
                        break;
                    }
                }
                results
            }
            ExecutionStrategy::Parallel => {
                let max_concurrency = self.exec_config.max_concurrency.unwrap_or(4) as usize;
                stream::iter(executable.to_vec())
                    .map(|name| async move {
                        let result = self.execute_pipeline(&name).await;
                        (name, result)
                    })
                    .buffer_unordered(max_concurrency)
                    .collect()
                    .await
            }
        };

        for (name, result) in results {
            match result {
                Ok(rows) => {
                    info!("Pipeline '{}' completed successfully ({} rows)", name, rows);
                    self.mark_pipeline_completed(&name, rows, run_state, completed_pipelines)
                        .await?;
                }
                Err(MigrationError::Paused) => {
                    info!("Pipeline '{}' paused at batch boundary", name);
                    return Err(MigrationError::Paused);
                }
                Err(MigrationError::ShutdownRequested) => {
                    info!("Pipeline '{}' stopped due to shutdown", name);
                    return Err(MigrationError::ShutdownRequested);
                }
                Err(e) => {
                    error!("Pipeline '{}' failed: {}", name, e);
                    failed_pipelines.insert(name.clone());

                    if matches!(self.exec_config.on_failure, FailureStrategy::FailFast) {
                        return Err(MigrationError::PipelinesFailed(vec![name]));
                    }
                }
            }
        }

        Ok(())
    }

    /// Mark a pipeline as completed in the RunState and persist to sled.
    async fn mark_pipeline_completed(
        &self,
        name: &str,
        rows_done: u64,
        run_state: &mut RunState,
        completed_pipelines: &mut HashSet<String>,
    ) -> Result<(), MigrationError> {
        completed_pipelines.insert(name.to_string());

        if let Some(ps) = run_state.pipelines.iter_mut().find(|p| p.name == name) {
            ps.status = PipelineStatus::Completed;
            // Read cumulative rows from checkpoint (authoritative source),
            // fall back to prior count + current session metrics.
            if let Ok(Some(cp)) = self
                .exec_ctx
                .state
                .load_checkpoint(&self.exec_ctx.run_id(), &ps.item_id, "part-0")
                .await
            {
                ps.rows_done = cp.rows_done;
            } else {
                ps.rows_done += rows_done;
            }
        }

        self.exec_ctx.state.save_run_state(run_state).await?;
        Ok(())
    }

    /// Save RunState as paused with current pipeline statuses.
    async fn save_paused_state(
        &self,
        base_state: &engine_state::models::RunState,
        failed_pipelines: &HashSet<String>,
        completed_pipelines: &HashSet<String>,
        reason: engine_state::models::PauseReason,
    ) -> Result<(), MigrationError> {
        use engine_state::models::{PipelineStatus, RunStatus};

        let mut state = base_state.clone();
        state.status = RunStatus::Paused {
            reason: reason.clone(),
            paused_at: chrono::Utc::now(),
        };

        let run_id = self.exec_ctx.run_id();
        for ps in &mut state.pipelines {
            if completed_pipelines.contains(&ps.name) {
                ps.status = PipelineStatus::Completed;
            } else if failed_pipelines.contains(&ps.name) {
                ps.status = PipelineStatus::Failed {
                    error: "Failed during execution".to_string(),
                };
            }

            // Update rows_done from checkpoint (cumulative) for all pipelines
            if let Ok(Some(cp)) = self
                .exec_ctx
                .state
                .load_checkpoint(&run_id, &ps.item_id, "part-0")
                .await
            {
                ps.rows_done = cp.rows_done;
            }
        }

        self.exec_ctx.state.save_run_state(&state).await?;
        self.exec_ctx
            .state
            .append_wal(&WalEntry::RunPaused {
                run_id: self.exec_ctx.run_id(),
                reason,
            })
            .await?;

        Ok(())
    }

    /// Returns (rows_processed) on success.
    async fn execute_pipeline(&self, pipeline_name: &str) -> Result<u64, MigrationError> {
        let (idx, pipeline) = self
            .plan
            .pipelines
            .iter()
            .enumerate()
            .find(|(_, p)| p.name == pipeline_name)
            .ok_or_else(|| {
                MigrationError::PipelineFailed(format!("Pipeline '{}' not found", pipeline_name))
            })?;

        self.run_pipeline(idx, pipeline).await
    }

    async fn run_pipeline(&self, idx: usize, pipeline: &Pipeline) -> Result<u64, MigrationError> {
        let start_time = std::time::Instant::now();
        info!("Starting migration pipeline {}", pipeline.destination.table);

        // Resolve drivers and core mapping components
        let src_driver = self
            .exec_ctx
            .resolve_driver(&pipeline.source.connection)
            .await?;
        let dst_driver = self
            .exec_ctx
            .resolve_driver(&pipeline.destination.connection)
            .await?;
        let mapping = TransformationMetadata::new(pipeline);
        let offset_strategy = OffsetStrategyFactory::from_pagination(&pipeline.source.pagination);

        // Prepare Source and Destination (including graph expansion)
        let (source, destination, expanded_schema_ops, cascade_tables) = self
            .prepare_endpoints(
                pipeline,
                &src_driver,
                &dst_driver,
                &mapping,
                offset_strategy.clone(),
            )
            .await?;

        // Build context and log start
        let mut pipeline_ctx = self
            .create_pipeline_context(idx, pipeline, source, destination, mapping, offset_strategy)
            .await?;

        // Validate settings and collect schema ops (no DDL execution)
        let (settings, mut schema_ops) = self
            .validate_and_plan_settings(&mut pipeline_ctx, &src_driver, &dst_driver, pipeline)
            .await?;

        // If graph expansion produced schema ops, they replace the settings-based ops
        // (graph expansion produces properly topologically sorted DDL)
        if let Some(expanded_ops) = expanded_schema_ops {
            schema_ops = expanded_ops;
        }

        let orchestrator = PipelineOrchestrator::new(
            pipeline.clone(),
            pipeline_ctx,
            dst_driver,
            settings,
            schema_ops,
            self.shutdown.clone(),
            self.event_bus.clone(),
            self.done_ops.clone(),
            cascade_tables,
        );

        // Execute: pre-DDL -> data migration -> post-DDL
        let rows = orchestrator.execute().await?;

        info!(
            "Migration item {} completed in {:.2}s ({} rows)",
            pipeline.destination.table,
            start_time.elapsed().as_secs_f64(),
            rows
        );

        Ok(rows)
    }

    async fn prepare_endpoints(
        &self,
        pipeline: &Pipeline,
        src_driver: &DriverRef,
        dst_driver: &DriverRef,
        mapping: &TransformationMetadata,
        offset_strategy: Arc<dyn OffsetStrategy>,
    ) -> Result<(Source, Destination, Option<SchemaOps>, Vec<String>), MigrationError> {
        let (expanded_schema_ops, cascade_meta) = self
            .get_graph_expansion(pipeline, src_driver, mapping)
            .await?;
        let cascade_tables = resolve_cascade_tables(pipeline, mapping, &cascade_meta);

        let source = dispatch_driver!(src_driver, |d| {
            Source::with_cascade(d.clone(), pipeline, mapping, offset_strategy, cascade_meta).await
        })?;

        let destination = dispatch_driver!(dst_driver, |d| {
            d.clone()
                .into_destination(&pipeline.destination.table, src_driver.dialect())
        });

        Ok((source, destination, expanded_schema_ops, cascade_tables))
    }

    async fn get_graph_expansion(
        &self,
        pipeline: &Pipeline,
        src_driver: &DriverRef,
        mapping: &TransformationMetadata,
    ) -> Result<(Option<SchemaOps>, Option<HashMap<String, TableMetadata>>), MigrationError> {
        if let Some(refs) = &pipeline.source.graph_references {
            self.expand_graph_references(&pipeline.source.table, src_driver, mapping, refs)
                .await
        } else {
            Ok((None, None))
        }
    }

    /// Expand graph references: introspect FK dependencies and produce schema ops + cascade metadata.
    async fn expand_graph_references(
        &self,
        root_table: &str,
        src_driver: &DriverRef,
        mapping: &TransformationMetadata,
        refs: &GraphReferences,
    ) -> Result<(Option<SchemaOps>, Option<HashMap<String, TableMetadata>>), MigrationError> {
        let source_dialect = src_driver.dialect();

        let result = dispatch_driver!(src_driver, |d| {
            let introspector: Arc<dyn SchemaIntrospector> = d.clone() as _;
            let type_registry = Arc::new(TypeRegistry::new(
                source_dialect,
                Dialect::Postgres, // TODO: derive from destination driver
            ));

            let expander = GraphExpander::new(introspector, type_registry, source_dialect);
            expander
                .expand(root_table, refs, mapping, false, false)
                .await
                .map_err(MigrationError::from)?
        });

        let cascade_meta = if matches!(refs.data_mode, DataMode::Cascade) {
            Some(result.discovered_tables)
        } else {
            None
        };

        Ok((Some(result.schema_ops), cascade_meta))
    }

    /// Initializes the `PipelineContext` and commits the initialization event to the WAL.
    async fn create_pipeline_context(
        &self,
        idx: usize,
        pipeline: &Pipeline,
        source: Source,
        destination: Destination,
        mapping: TransformationMetadata,
        offset_strategy: Arc<dyn OffsetStrategy>,
    ) -> Result<PipelineContext, MigrationError> {
        let exec_ctx = Arc::new(self.exec_ctx.clone());
        let run_id = self.exec_ctx.run_id();
        let item_id = make_item_id(self.plan.hash(), &pipeline.destination.table, idx);

        let pipeline_ctx = PipelineContext::builder(exec_ctx.clone())
            .run_id(run_id.clone())
            .item_id(item_id.clone())
            .source(source)
            .destination(destination)
            .pipeline(pipeline.clone())
            .mapping(mapping)
            .state(self.exec_ctx.state.clone())
            .offset_strategy(offset_strategy)
            .cursor(Cursor::None)
            .build();

        pipeline_ctx
            .state
            .append_wal(&WalEntry::ItemStart { run_id, item_id })
            .await?;

        Ok(pipeline_ctx)
    }

    /// Validate settings and collect schema operations without executing DDL.
    async fn validate_and_plan_settings(
        &self,
        ctx: &mut PipelineContext,
        src_driver: &DriverRef,
        dst_driver: &DriverRef,
        pipeline: &Pipeline,
    ) -> Result<(ValidatedSettings, SchemaOps), MigrationError> {
        let result = dispatch_drivers!(src_driver, dst_driver, |src, dst| {
            settings::validate_and_plan::<Src, Dst>(
                ctx,
                src.clone(),
                dst.clone(),
                &pipeline.settings,
                self.flags.dry_run,
                self.flags.integrity,
            )
            .await?
        });

        Ok(result)
    }
}
