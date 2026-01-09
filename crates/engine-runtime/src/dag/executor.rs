use crate::{
    dag::Dag,
    error::MigrationError,
    execution::{metadata, orchestrator::PipelineOrchestrator},
};
use engine_config::settings;
use engine_core::{
    connectors::{destination::Destination, source::Source},
    context::{exec::ExecutionContext, item::ItemContext},
    event_bus::bus::EventBus,
    plan::execution::ExecutionPlan,
    state::{StateStore, models::WalEntry, sled_store::SledStateStore},
    utils::make_item_id,
};
use futures::lock::Mutex;
use futures::stream::{self, StreamExt};
use model::{
    events::migration::MigrationEvent,
    execution::{
        execution_config::{ExecutionConfig, ExecutionStrategy, FailureStrategy},
        pipeline::Pipeline,
    },
};
use model::{pagination::cursor::Cursor, transform::mapping::TransformationMetadata};
use query_builder::offsets::OffsetStrategyFactory;
use std::{collections::HashSet, sync::Arc};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

pub struct DagExecutor {
    plan: ExecutionPlan,
    dry_run: bool,
    cancel: CancellationToken,
    exec_ctx: ExecutionContext,
    exec_config: ExecutionConfig,
    event_bus: EventBus,
}

impl DagExecutor {
    /// Create executor without event bus (headless mode)
    pub async fn new(
        plan: ExecutionPlan,
        dry_run: bool,
        cancel: CancellationToken,
    ) -> Result<Self, MigrationError> {
        // Create default event bus for internal monitoring
        let event_bus = EventBus::new();
        Self::subscribe_to_events(&event_bus).await;

        Self::init(plan, dry_run, cancel, event_bus).await
    }

    /// Create executor with event bus for external monitoring (TUI/Pretty mode)
    pub async fn with_event_bus(
        plan: ExecutionPlan,
        dry_run: bool,
        cancel: CancellationToken,
        event_bus: EventBus,
    ) -> Result<Self, MigrationError> {
        Self::init(plan, dry_run, cancel, event_bus).await
    }

    pub async fn execute(self, dag: Dag) -> Result<(), MigrationError> {
        // Track failed pipelines to skip their dependents
        let mut failed_pipelines = HashSet::new();

        self.exec_ctx
            .state
            .append_wal(&WalEntry::RunStart {
                run_id: self.exec_ctx.run_id(),
                plan_hash: self.plan.hash(),
            })
            .await?;

        // Execute pipelines in DAG order
        let execution_order = dag.execution_order();
        info!(
            "Executing {} pipelines in {} levels",
            dag.total_pipelines(),
            execution_order.len()
        );

        for (level_idx, level) in execution_order.iter().enumerate() {
            // Filter out pipelines whose dependencies failed
            let executable: Vec<String> = level
                .iter()
                .filter(|name| {
                    let pipeline = self
                        .plan
                        .pipelines
                        .iter()
                        .find(|p| &p.name == *name)
                        .unwrap();

                    // Skip if any dependency failed
                    let has_failed_dep = pipeline
                        .dependencies
                        .iter()
                        .any(|dep| failed_pipelines.contains(dep));

                    if has_failed_dep {
                        warn!("Skipping pipeline '{}' due to failed dependency", name);
                    }

                    !has_failed_dep
                })
                .cloned()
                .collect();

            // Add skipped pipelines to failed_pipelines so their dependents are also skipped
            for pipeline_name in level {
                if !executable.contains(pipeline_name) {
                    failed_pipelines.insert(pipeline_name.clone());
                }
            }

            if executable.is_empty() {
                warn!("All pipelines in level {} skipped", level_idx + 1);
                continue;
            }

            info!(
                "Level {}/{}: Executing {} pipelines (skipped {}): {:?}",
                level_idx + 1,
                dag.execution_order().len(),
                executable.len(),
                level.len() - executable.len(),
                executable
            );

            // Check cancellation
            if self.cancel.is_cancelled() {
                warn!("Shutdown requested");
                return Err(MigrationError::ShutdownRequested);
            }

            match self.exec_config.strategy {
                ExecutionStrategy::Sequential => {
                    for pipeline_name in &executable {
                        match self.execute_pipeline(pipeline_name).await {
                            Ok(_) => {
                                info!("Pipeline '{}' completed successfully", pipeline_name);
                            }
                            Err(e) => {
                                error!("Pipeline '{}' failed: {}", pipeline_name, e);
                                failed_pipelines.insert(pipeline_name.clone());

                                if matches!(self.exec_config.on_failure, FailureStrategy::FailFast)
                                {
                                    return Err(e);
                                }
                            }
                        }
                    }
                }
                ExecutionStrategy::Parallel => {
                    let max_concurrency = self.exec_config.max_concurrency.unwrap_or(4); // Default to 4 if not set
                    let results = self
                        .execute_level_parallel(&executable, max_concurrency as usize)
                        .await;

                    for (name, result) in results {
                        match result {
                            Ok(_) => {
                                info!("Pipeline '{}' completed successfully", name);
                            }
                            Err(e) => {
                                error!("Pipeline '{}' failed: {}", name, e);
                                failed_pipelines.insert(name.clone());

                                if matches!(self.exec_config.on_failure, FailureStrategy::FailFast)
                                {
                                    return Err(MigrationError::PipelinesFailed(vec![name]));
                                }
                            }
                        }
                    }
                }
            }
        }

        if !failed_pipelines.is_empty() {
            let failed_list: Vec<String> = failed_pipelines.into_iter().collect();
            error!(
                "Migration completed with {} failed/skipped pipelines: {:?}",
                failed_list.len(),
                failed_list
            );

            // With "continue" strategy, log failures but don't return error
            // With "fail_fast" strategy, we would have already returned early
            match self.exec_config.on_failure {
                FailureStrategy::Continue => {
                    warn!(
                        "Continue strategy: returning Ok despite {} failed/skipped pipelines",
                        failed_list.len()
                    );
                    Ok(())
                }
                FailureStrategy::FailFast => {
                    // This shouldn't happen since fail_fast returns early, but handle it anyway
                    Err(MigrationError::PipelinesFailed(failed_list))
                }
            }
        } else {
            info!("Migration completed successfully with all pipelines");
            Ok(())
        }
    }

    async fn init(
        plan: ExecutionPlan,
        dry_run: bool,
        cancel: CancellationToken,
        event_bus: EventBus,
    ) -> Result<Self, MigrationError> {
        let home_dir = dirs::home_dir().ok_or_else(|| {
            MigrationError::InitializationError("Could not determine home directory".to_string())
        })?;

        let state = Arc::new(SledStateStore::open(home_dir.join(".stratum/state"))?);
        let exec_ctx = ExecutionContext::new(&plan, state).await?;
        let exec_config = plan.execution_config.clone();

        Ok(Self {
            plan,
            dry_run,
            cancel,
            exec_ctx,
            exec_config,
            event_bus,
        })
    }

    async fn execute_level_parallel(
        &self,
        pipelines: &[String],
        max_concurrency: usize,
    ) -> Vec<(String, Result<(), MigrationError>)> {
        let pipeline_names: Vec<String> = pipelines.to_vec();

        stream::iter(pipeline_names)
            .map(|pipeline_name| {
                let name = pipeline_name.clone();
                async move {
                    let result = self.execute_pipeline(&name).await;
                    (name, result)
                }
            })
            .buffer_unordered(max_concurrency)
            .collect()
            .await
    }

    async fn execute_pipeline(&self, pipeline_name: &str) -> Result<(), MigrationError> {
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

    async fn run_pipeline(&self, idx: usize, pipeline: &Pipeline) -> Result<(), MigrationError> {
        let start_time = std::time::Instant::now();
        info!("Starting migration pipeline {}", pipeline.destination.table);

        // Prepare context
        let exec_ctx = Arc::new(self.exec_ctx.clone());
        let run_id = self.exec_ctx.run_id();
        let item_id = make_item_id(&self.plan.hash(), &pipeline.destination.table, idx);
        let offset_strategy = OffsetStrategyFactory::from_pagination(&pipeline.source.pagination);
        let cursor = Cursor::None;

        // Create sources, destinations, and mapping
        let mapping = TransformationMetadata::new(pipeline);
        let source_adapter = self
            .exec_ctx
            .get_adapter(&pipeline.source.connection)
            .await?;
        let dest_adapter = self
            .exec_ctx
            .get_adapter(&pipeline.destination.connection)
            .await?;

        let source =
            Source::new(source_adapter, pipeline, &mapping, offset_strategy.clone()).await?;
        let destination = Destination::new(
            dest_adapter,
            &pipeline.destination.table,
            &pipeline.destination.connection,
        )
        .await?;

        let state = self.exec_ctx.state.clone();
        let mut item_ctx = ItemContext::builder(exec_ctx.clone())
            .run_id(run_id.clone())
            .item_id(item_id.clone())
            .source(source.clone())
            .destination(destination.clone())
            .pipeline(pipeline.clone())
            .mapping(mapping.clone())
            .state(state.clone())
            .offset_strategy(offset_strategy.clone())
            .cursor(cursor)
            .build();

        item_ctx
            .state
            .append_wal(&WalEntry::ItemStart { run_id, item_id })
            .await?;

        // Validate and apply settings
        let settings =
            settings::validate_and_apply(&mut item_ctx, &pipeline.settings, self.dry_run).await?;
        metadata::load(&mut item_ctx).await?;

        let item_ctx = Arc::new(Mutex::new(item_ctx));

        // Create and execute the pipeline orchestrator
        // This handles: before hooks -> pipeline execution -> after hooks
        let orchestrator = PipelineOrchestrator::new(
            pipeline.clone(),
            item_ctx,
            settings,
            self.cancel.clone(),
            self.event_bus.clone(),
        );

        orchestrator.execute().await?;

        let duration = start_time.elapsed();
        info!(
            "Migration item {} completed in {:.2}s",
            pipeline.destination.table,
            duration.as_secs_f64()
        );

        Ok(())
    }

    async fn subscribe_to_events(event_bus: &EventBus) {
        let (event_tx, mut event_rx) = mpsc::channel::<Arc<MigrationEvent>>(100);

        event_bus.subscribe::<MigrationEvent>(event_tx).await;

        // Spawn background task to handle all migration events
        tokio::spawn(async move {
            while let Some(event) = event_rx.recv().await {
                // TODO: Add more sophisticated event handling (logging, metrics, etc.)
                info!("Migration Event: {}", event);
            }
        });

        info!("Event subscriber configured for migration monitoring");
    }
}
