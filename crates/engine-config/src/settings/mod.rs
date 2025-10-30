use crate::{report::dry_run::DryRunReport, settings::error::SettingsError};
use async_trait::async_trait;
use batch_size::BatchSizeSetting;
use cascade::CascadeSchemaSetting;
use constraints::IgnoreConstraintsSettings;
use copy_cols::CopyColumnsSetting;
use create_cols::CreateMissingColumnsSetting;
use create_tables::CreateMissingTablesSetting;
use engine_core::context::item::ItemContext;
use futures::lock::Mutex;
use infer_schema::InferSchemaSetting;
use phase::MigrationSettingsPhase;
use smql_syntax::ast::setting::Settings;
use std::sync::Arc;

pub mod batch_size;
pub mod cascade;
pub mod constraints;
pub mod context;
pub mod copy_cols;
pub mod create_cols;
pub mod create_tables;
pub mod error;
pub mod infer_schema;
pub mod phase;
pub mod schema_manager;

#[async_trait]
pub trait MigrationSetting: Send + Sync {
    fn phase(&self) -> MigrationSettingsPhase;

    /// Check whether this setting should be applied in the given context.
    /// By default, all settings are applicable.
    fn can_apply(&self, _ctx: &ItemContext) -> bool {
        true
    }

    async fn apply(&mut self, ctx: &mut ItemContext) -> Result<(), SettingsError>;
}

pub async fn collect_settings(
    cfg: &Settings,
    ctx: &ItemContext,
    dry_run_report: &Arc<Mutex<Option<DryRunReport>>>,
) -> Vec<Box<dyn MigrationSetting>> {
    let src = ctx.source.clone();
    let dest = ctx.destination.clone();
    let state = ctx.state.clone();
    let mapping = ctx.mapping.clone();

    let mut all_settings: Vec<Box<dyn MigrationSetting>> = Vec::new();

    println!("Settings: {cfg:#?}");

    if cfg.batch_size > 0 {
        let batch_size_setting = BatchSizeSetting(cfg.batch_size as i64);
        all_settings.push(Box::new(batch_size_setting) as _);
    }

    if cfg.create_missing_columns {
        let missing_cols_setting =
            CreateMissingColumnsSetting::new(&src, &dest, &mapping, &state, dry_run_report).await;
        all_settings.push(Box::new(missing_cols_setting) as _);
    }

    if cfg.ignore_constraints {
        let ignore_constraints_setting = IgnoreConstraintsSettings(true);
        all_settings.push(Box::new(ignore_constraints_setting) as _);
    }

    if cfg.create_missing_tables {
        let missing_tables_setting =
            CreateMissingTablesSetting::new(&src, &dest, &mapping, &state, dry_run_report).await;
        all_settings.push(Box::new(missing_tables_setting) as _);
    }

    if cfg.infer_schema {
        let infer_schema_setting =
            InferSchemaSetting::new(&src, &dest, &mapping, &state, dry_run_report).await;
        all_settings.push(Box::new(infer_schema_setting) as _);
    }

    if cfg.cascade_schema {
        let cascade_schema_setting =
            CascadeSchemaSetting::new(&src, &dest, &mapping, &state, dry_run_report).await;
        all_settings.push(Box::new(cascade_schema_setting) as _);
    }

    let copy_columns_setting = CopyColumnsSetting(cfg.copy_columns);
    all_settings.push(Box::new(copy_columns_setting) as _);

    // Sort settings by phase to ensure they are applied in the correct order
    all_settings.sort_by_key(|s| s.phase());

    all_settings
}
