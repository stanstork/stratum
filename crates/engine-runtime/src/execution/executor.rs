use crate::{
    error::MigrationError,
    execution::{
        factory,
        metadata::{self},
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
        global::GlobalContext,
        item::{ItemContext, ItemContextParams},
    },
    state::{StateStore, models::WalEntry, sled_store::SledStateStore},
};
use futures::lock::Mutex;
use model::{pagination::cursor::Cursor, transform::mapping::EntityMapping};
use planner::{plan::MigrationPlan, query::offsets::OffsetStrategyFactory};
use smql_syntax::ast_v2::migrate::MigrateItem;
use std::{collections::HashMap, sync::Arc};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

pub async fn run(
    plan: MigrationPlan,
    dry_run: bool,
    cancel: CancellationToken,
) -> Result<HashMap<String, SummaryReport>, MigrationError> {
    MigrationExecutor::new(plan, dry_run, cancel)
        .await?
        .execute()
        .await
}

struct MigrationExecutor {
    plan: MigrationPlan,
    dry_run: bool,
    cancel: CancellationToken,
    global_ctx: GlobalContext,
}

impl MigrationExecutor {
    async fn new(
        plan: MigrationPlan,
        dry_run: bool,
        cancel: CancellationToken,
    ) -> Result<Self, MigrationError> {
        let home_dir = dirs::home_dir().ok_or_else(|| {
            MigrationError::InitializationError("Could not determine home directory".to_string())
        })?;
        let state = Arc::new(SledStateStore::open(home_dir.join(".stratum/state"))?);
        let global_ctx = GlobalContext::new(&plan, state).await?;

        Ok(Self {
            plan,
            dry_run,
            cancel,
            global_ctx,
        })
    }

    async fn execute(self) -> Result<HashMap<String, SummaryReport>, MigrationError> {
        info!("Running migration v2");
        info!("Migration run ID: {}", self.global_ctx.run_id());

        self.global_ctx
            .state
            .append_wal(&WalEntry::RunStart {
                run_id: self.global_ctx.run_id(),
                plan_hash: self.plan.hash(),
            })
            .await?;

        let mut report = HashMap::new();
        let total_items = self.plan.migration.migrate_items.len();

        for (idx, mi) in self.plan.migration.migrate_items.iter().enumerate() {
            // Check if shutdown was requested before starting next item
            if self.cancel.is_cancelled() {
                warn!(
                    "Shutdown requested before starting item {}/{}: {}",
                    idx + 1,
                    total_items,
                    mi.destination.name()
                );
                info!("Stopping migration gracefully - partial progress saved");
                return Err(MigrationError::ShutdownRequested);
            }

            info!(
                "Processing item {}/{}: {}",
                idx + 1,
                total_items,
                mi.destination.name()
            );

            let summary = self.run_item(idx, mi).await?;
            report.insert(mi.destination.name().clone(), summary);
        }

        info!("Migration completed");
        Ok(report)
    }

    async fn run_item(
        &self,
        idx: usize,
        mi: &MigrateItem,
    ) -> Result<SummaryReport, MigrationError> {
        let start_time = std::time::Instant::now();
        info!("Starting migration item {}", mi.destination.name());

        let run_id = self.global_ctx.run_id();
        let item_id = Self::make_item_id(&self.plan.hash(), mi, idx);

        let offset_strategy = OffsetStrategyFactory::from_smql(&mi.offset);
        let cursor = Cursor::None;

        let mapping = EntityMapping::new(mi);
        let source = factory::create_source(
            &self.global_ctx,
            &self.plan.connections,
            &mapping,
            mi,
            offset_strategy.clone(),
        )
        .await?;
        let destination =
            factory::create_destination(&self.global_ctx, &self.plan.connections, mi).await?;

        let state = self.global_ctx.state.clone();
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

        let dry_run_report = self.dry_run_report(&source, &destination, &mapping, mi);

        let settings = settings::validate_and_apply(
            &mut item_ctx,
            &mi.settings,
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
            mi.destination.name(),
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
        mapping: &EntityMapping,
        mi: &MigrateItem,
    ) -> Arc<Mutex<DryRunReport>> {
        Arc::new(Mutex::new(DryRunReport::new(DryRunParams {
            source: source_endpoint(source),
            destination: dest_endpoint(destination),
            mapping,
            config_hash: &self.plan.hash(),
            copy_columns: mi.settings.copy_columns,
        })))
    }

    fn make_item_id(plan_hash: &str, mi: &MigrateItem, idx: usize) -> String {
        // Stable & human-ish: plan-hash + item-index + dest-name
        let mut h = blake3::Hasher::new();
        h.update(plan_hash.as_bytes());
        h.update(b":");
        h.update(idx.to_string().as_bytes());
        h.update(b":");
        h.update(mi.destination.name().as_bytes());
        format!("itm-{}", &h.finalize().to_hex()[..16])
    }
}
