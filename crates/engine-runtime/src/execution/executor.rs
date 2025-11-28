use crate::{
    error::MigrationError,
    execution::{
        factory,
        metadata::{self},
        settings::{self},
        workers,
    },
};
use engine_config::report::{
    dry_run::{DryRunParams, DryRunReport, dest_endpoint, source_endpoint},
    summary::SummaryReport,
};
use engine_core::{
    connectors::{destination::Destination, source::Source},
    context::{
        global::GlobalContext,
        item::{ItemContext, ItemContextParams},
    },
    migration_state::MigrationSettings,
    state::{StateStore, models::WalEntry, sled_store::SledStateStore},
};
use futures::lock::Mutex;
use model::{pagination::cursor::Cursor, transform::mapping::EntityMapping};
use planner::{plan::MigrationPlan, query::offsets::OffsetStrategyFactory};
use smql_syntax::ast::migrate::MigrateItem;
use std::{collections::HashMap, sync::Arc};
use tokio_util::sync::CancellationToken;
use tracing::info;

pub async fn run(
    plan: MigrationPlan,
    dry_run: bool,
) -> Result<HashMap<String, SummaryReport>, MigrationError> {
    MigrationExecutor::new(plan, dry_run).await?.execute().await
}

struct MigrationExecutor {
    plan: MigrationPlan,
    dry_run: bool,
    global_ctx: GlobalContext,
}

impl MigrationExecutor {
    async fn new(plan: MigrationPlan, dry_run: bool) -> Result<Self, MigrationError> {
        let home_dir = dirs::home_dir().ok_or_else(|| {
            MigrationError::InitializationError("Could not determine home directory".to_string())
        })?;
        let state = Arc::new(SledStateStore::open(home_dir.join(".stratum/state"))?);
        let global_ctx = GlobalContext::new(&plan, state).await?;

        Ok(Self {
            plan,
            dry_run,
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
        for (idx, mi) in self.plan.migration.migrate_items.iter().enumerate() {
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
        let settings = MigrationSettings::new(self.dry_run);
        let mut item_ctx = ItemContext::new(ItemContextParams {
            run_id: run_id.clone(),
            item_id: item_id.clone(),
            source: source.clone(),
            destination: destination.clone(),
            mapping: mapping.clone(),
            state: state.clone(),
            offset_strategy: offset_strategy.clone(),
            cursor,
            settings,
        });

        item_ctx
            .state
            .append_wal(&WalEntry::ItemStart { run_id, item_id })
            .await?;

        let dry_run_report = self.dry_run_report(&source, &destination, &mapping, mi);

        settings::apply_all(&mut item_ctx, &mi.settings, &dry_run_report).await?;
        metadata::load(&mut item_ctx).await?;

        let ctx = Arc::new(Mutex::new(item_ctx));
        let cancel = CancellationToken::new();
        workers::spawn(ctx, &mi.settings, cancel, &dry_run_report).await?;

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
