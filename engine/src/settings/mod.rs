use crate::{context::item::ItemContext, error::MigrationError};
use async_trait::async_trait;
use batch_size::BatchSizeSetting;
use cascade::CascadeSchemaSetting;
use constraints::IgnoreConstraintsSettings;
use copy_cols::CopyColumnsSetting;
use create_cols::CreateMissingColumnsSetting;
use create_tables::CreateMissingTablesSetting;
use infer_schema::InferSchemaSetting;
use phase::MigrationSettingsPhase;
use smql::statements::setting::Settings;

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

#[async_trait]
pub trait MigrationSetting: Send + Sync {
    fn phase(&self) -> MigrationSettingsPhase;

    /// Check whether this setting should be applied in the given context.
    /// By default, all settings are applicable.
    fn can_apply(&self, _ctx: &ItemContext) -> bool {
        true
    }

    async fn apply(&self, ctx: &mut ItemContext) -> Result<(), MigrationError>;
}

pub fn collect_settings(cfg: &Settings, ctx: &ItemContext) -> Vec<Box<dyn MigrationSetting>> {
    let src = ctx.source.clone();
    let dest = ctx.destination.clone();
    let state = ctx.state.clone();
    let mapping = ctx.mapping.clone();

    // Collect all settings based on the configuration
    let mut all: Vec<Box<dyn MigrationSetting>> = [
        // batch size > 0?
        cfg.batch_size
            .gt(&0)
            .then(|| Box::new(BatchSizeSetting(cfg.batch_size as i64)) as _),
        // ignore constraints?
        cfg.ignore_constraints
            .then(|| Box::new(IgnoreConstraintsSettings(true)) as _),
        // create missing tables?
        cfg.create_missing_tables.then(|| {
            Box::new(CreateMissingTablesSetting::new(
                &src, &dest, &mapping, &state,
            )) as _
        }),
        // create missing columns?
        cfg.create_missing_columns.then(|| {
            Box::new(CreateMissingColumnsSetting::new(
                &src, &dest, &mapping, &state,
            )) as _
        }),
        // infer schema?
        cfg.infer_schema
            .then(|| Box::new(InferSchemaSetting::new(&src, &dest, &mapping, &state)) as _),
        // cascade schema?
        cfg.cascade_schema
            .then(|| Box::new(CascadeSchemaSetting::new(&src, &dest, &mapping, &state)) as _),
        // copy columns
        Some(Box::new(CopyColumnsSetting(cfg.copy_columns.clone())) as _),
    ]
    .into_iter()
    .flatten()
    .collect();

    // Sort settings by phase to ensure they are applied in the correct order
    all.sort_by_key(|s| s.phase());

    all
}
