use crate::{
    error::MigrationError,
    execution::{
        factory,
        metadata::{self},
        utils::offset_from_pagination,
        workers,
    },
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
    context::{
        exec::ExecutionContext,
        item::{ItemContext, ItemContextParams},
    },
    plan::ExecutionPlan,
    state::{StateStore, models::WalEntry, sled_store::SledStateStore},
};
use futures::lock::Mutex;
use model::execution::pipeline::Pipeline;
use model::{pagination::cursor::Cursor, transform::mapping::TransformationMetadata};
use std::{collections::HashMap, sync::Arc};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

pub async fn run(
    plan: ExecutionPlan,
    dry_run: bool,
    cancel: CancellationToken,
) -> Result<HashMap<String, SummaryReport>, MigrationError> {
    MigrationExecutor::new(plan, dry_run, cancel)
        .await?
        .execute()
        .await
}

struct MigrationExecutor {
    plan: ExecutionPlan,
    dry_run: bool,
    cancel: CancellationToken,
    exec_ctx: ExecutionContext,
}

impl MigrationExecutor {
    async fn new(
        plan: ExecutionPlan,
        dry_run: bool,
        cancel: CancellationToken,
    ) -> Result<Self, MigrationError> {
        let home_dir = dirs::home_dir().ok_or_else(|| {
            MigrationError::InitializationError("Could not determine home directory".to_string())
        })?;
        let state = Arc::new(SledStateStore::open(home_dir.join(".stratum/state"))?);
        let exec_ctx = ExecutionContext::new(&plan, state).await?;

        Ok(Self {
            plan,
            dry_run,
            cancel,
            exec_ctx,
        })
    }

    async fn execute(self) -> Result<HashMap<String, SummaryReport>, MigrationError> {
        info!("Running migration v2");
        info!("Migration run ID: {}", self.exec_ctx.run_id());

        self.exec_ctx
            .state
            .append_wal(&WalEntry::RunStart {
                run_id: self.exec_ctx.run_id(),
                plan_hash: self.plan.hash(),
            })
            .await?;

        let mut report = HashMap::new();
        let total_items = self.plan.pipelines.len();

        for (idx, p) in self.plan.pipelines.iter().enumerate() {
            // Check if shutdown was requested before starting next item
            if self.cancel.is_cancelled() {
                warn!(
                    "Shutdown requested before starting pipeline {}/{}: {}",
                    idx + 1,
                    total_items,
                    p.destination.table
                );
                info!("Stopping migration gracefully - partial progress saved");
                return Err(MigrationError::ShutdownRequested);
            }

            info!(
                "Processing pipeline {}/{}: {}",
                idx + 1,
                total_items,
                p.destination.table
            );

            let summary = self.run_pipeline(idx, p).await?;
            report.insert(p.destination.table.clone(), summary);
        }

        info!("Migration completed");
        Ok(report)
    }

    async fn run_pipeline(
        &self,
        idx: usize,
        pipeline: &Pipeline,
    ) -> Result<SummaryReport, MigrationError> {
        let start_time = std::time::Instant::now();
        info!("Starting migration pipeline {}", pipeline.destination.table);
        let run_id = self.exec_ctx.run_id();
        let item_id = Self::make_item_id(&self.plan.hash(), pipeline, idx);

        let offset_strategy = offset_from_pagination(&pipeline.source.pagination);
        let cursor = Cursor::None;

        let mapping = TransformationMetadata::new(pipeline);
        let source =
            factory::create_source(&self.exec_ctx, &pipeline, &mapping, offset_strategy.clone())
                .await?;
        let destination = factory::create_destination(&self.exec_ctx, &pipeline).await?;

        let state = self.exec_ctx.state.clone();
        let mut item_ctx = ItemContext::new(ItemContextParams {
            run_id: run_id.clone(),
            item_id: item_id.clone(),
            source: source.clone(),
            destination: destination.clone(),
            mapping: mapping.clone(),
            state: state.clone(),
            offset_strategy: offset_strategy.clone(),
            cursor,
        });

        item_ctx
            .state
            .append_wal(&WalEntry::ItemStart { run_id, item_id })
            .await?;

        let dry_run_report = self.dry_run_report(&source, &destination, &mapping);

        let settings = settings::validate_and_apply(
            &mut item_ctx,
            &pipeline.settings,
            self.dry_run,
            &dry_run_report,
        )
        .await?;
        metadata::load(&mut item_ctx).await?;

        let ctx = Arc::new(Mutex::new(item_ctx));
        workers::spawn(ctx, &settings, self.cancel.clone(), &dry_run_report).await?;

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
