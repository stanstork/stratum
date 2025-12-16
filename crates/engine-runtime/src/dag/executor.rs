use crate::{
    dag::Dag,
    error::MigrationError,
    execution::{factory, metadata, orchestrator::PipelineOrchestrator},
};
use engine_config::{
    report::{
        dry_run::{DryRunParams, DryRunReport, dest_endpoint, source_endpoint},
        summary::SummaryReport,
    },
    settings,
};
use engine_core::{
    connectors::{destination::Destination, source::Source},
    context::{exec::ExecutionContext, item::ItemContext},
    plan::execution::ExecutionPlan,
    state::{StateStore, models::WalEntry, sled_store::SledStateStore},
};
use futures::lock::Mutex;
use futures::stream::{self, StreamExt};
use model::execution::{
    execution_config::{ExecutionConfig, ExecutionStrategy, FailureStrategy},
    pipeline::Pipeline,
};
use model::{pagination::cursor::Cursor, transform::mapping::TransformationMetadata};
use planner::query::offsets::OffsetStrategyFactory;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

pub struct DagExecutor {
    plan: ExecutionPlan,
    dry_run: bool,
    cancel: CancellationToken,
    exec_ctx: ExecutionContext,
    exec_config: ExecutionConfig,
}

impl DagExecutor {
    pub async fn new(
        plan: ExecutionPlan,
        dry_run: bool,
        cancel: CancellationToken,
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
        })
    }

    pub async fn execute(self, dag: Dag) -> Result<HashMap<String, SummaryReport>, MigrationError> {
        // Track failed pipelines to skip their dependents
        let mut failed_pipelines = HashSet::new();
        let mut report = HashMap::new();

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
                            Ok(summary) => {
                                report.insert(pipeline_name.clone(), summary);
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
                            Ok(summary) => {
                                report.insert(name.clone(), summary);
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
                    Ok(report)
                }
                FailureStrategy::FailFast => {
                    // This shouldn't happen since fail_fast returns early, but handle it anyway
                    Err(MigrationError::PipelinesFailed(failed_list))
                }
            }
        } else {
            Ok(report)
        }
    }

    async fn execute_level_parallel(
        &self,
        pipelines: &[String],
        max_concurrency: usize,
    ) -> Vec<(String, Result<SummaryReport, MigrationError>)> {
        stream::iter(pipelines)
            .map(|pipeline_name| async move {
                let name = pipeline_name.clone();
                let result = self.execute_pipeline(pipeline_name).await;
                (name, result)
            })
            .buffer_unordered(max_concurrency)
            .collect()
            .await
    }

    async fn execute_pipeline(&self, pipeline_name: &str) -> Result<SummaryReport, MigrationError> {
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

    async fn run_pipeline(
        &self,
        idx: usize,
        pipeline: &Pipeline,
    ) -> Result<SummaryReport, MigrationError> {
        let start_time = std::time::Instant::now();
        info!("Starting migration pipeline {}", pipeline.destination.table);

        // Prepare context
        let exec_ctx = Arc::new(self.exec_ctx.clone());
        let run_id = self.exec_ctx.run_id();
        let item_id = Self::make_item_id(&self.plan.hash(), pipeline, idx);
        let offset_strategy = OffsetStrategyFactory::from_pagination(&pipeline.source.pagination);
        let cursor = Cursor::None;

        // Create sources, destinations, and mapping
        let mapping = TransformationMetadata::new(pipeline);
        let source =
            factory::create_source(&self.exec_ctx, pipeline, &mapping, offset_strategy.clone())
                .await?;
        let destination = factory::create_destination(&self.exec_ctx, pipeline).await?;

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

        let dry_run_report = self.dry_run_report(&source, &destination, &mapping);

        // Validate and apply settings
        let settings = settings::validate_and_apply(
            &mut item_ctx,
            &pipeline.settings,
            self.dry_run,
            &dry_run_report,
        )
        .await?;
        metadata::load(&mut item_ctx).await?;

        let item_ctx = Arc::new(Mutex::new(item_ctx));

        // Create and execute the pipeline orchestrator
        // This handles: before hooks -> pipeline execution -> after hooks
        let orchestrator = PipelineOrchestrator::new(
            pipeline.clone(),
            item_ctx,
            settings,
            self.cancel.clone(),
            dry_run_report.clone(),
        );

        orchestrator.execute().await?;

        let duration = start_time.elapsed();
        info!(
            "Migration item {} completed in {:.2}s",
            pipeline.destination.table,
            duration.as_secs_f64()
        );

        let final_report = dry_run_report.lock().await.clone();
        Ok(SummaryReport {
            dry_run_report: self.dry_run.then_some(final_report),
        })
    }

    fn dry_run_report(
        &self,
        source: &Source,
        destination: &Destination,
        mapping: &TransformationMetadata,
    ) -> Arc<Mutex<DryRunReport>> {
        Arc::new(Mutex::new(DryRunReport::new(DryRunParams {
            source: source_endpoint(source),
            destination: dest_endpoint(destination),
            mapping,
            config_hash: &self.plan.hash(),
            copy_columns: engine_config::settings::CopyColumns::All,
        })))
    }

    fn make_item_id(plan_hash: &str, p: &Pipeline, idx: usize) -> String {
        // Stable & human-ish: plan-hash + item-index + dest-name
        let mut h = blake3::Hasher::new();
        h.update(plan_hash.as_bytes());
        h.update(b":");
        h.update(idx.to_string().as_bytes());
        h.update(b":");
        h.update(p.destination.table.as_bytes());
        format!("itm-{}", &h.finalize().to_hex()[..16])
    }
}
