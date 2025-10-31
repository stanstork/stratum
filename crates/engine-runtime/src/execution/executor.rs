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
    context::{global::GlobalContext, item::ItemContext},
    migration_state::MigrationState,
    state::sled_store::SledStateStore,
};
use futures::lock::Mutex;
use model::transform::mapping::EntityMapping;
use planner::plan::MigrationPlan;
use smql_syntax::ast::migrate::MigrateItem;
use std::{collections::HashMap, sync::Arc};
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
        let mut report = HashMap::new();

        for mi in self.plan.migration.migrate_items.iter() {
            let summary = self.run_item(mi).await?;
            report.insert(mi.destination.name().clone(), summary);
        }

        info!("Migration completed");
        Ok(report)
    }

    async fn run_item(&self, mi: &MigrateItem) -> Result<SummaryReport, MigrationError> {
        info!("Starting migration item {}", mi.destination.name());

        let mapping = EntityMapping::new(mi);
        let source =
            factory::create_source(&self.global_ctx, &self.plan.connections, &mapping, mi).await?;
        let destination =
            factory::create_destination(&self.global_ctx, &self.plan.connections, mi).await?;

        let dry_run_report = if self.dry_run {
            Arc::new(Mutex::new(Some(DryRunReport::new(DryRunParams {
                source: source_endpoint(&source),
                destination: dest_endpoint(&destination),
                mapping: &mapping,
                config_hash: &self.plan.hash(),
                copy_columns: mi.settings.copy_columns,
            }))))
        } else {
            Arc::new(Mutex::new(None))
        };

        let state = MigrationState::new(self.dry_run);
        let mut item_ctx = ItemContext::new(source, destination, mapping.clone(), state);

        settings::apply_all(&mut item_ctx, &mi.settings, &dry_run_report).await?;
        metadata::load(&mut item_ctx).await?;

        let ctx = Arc::new(Mutex::new(item_ctx));
        workers::spawn(ctx, &mi.settings, &dry_run_report).await?;

        info!("Migration item {} completed", mi.destination.name());

        let final_report = dry_run_report.lock().await.clone();
        Ok(SummaryReport {
            dry_run_report: final_report,
        })
    }
}
