use crate::{
    report::dry_run::DryRunReport,
    settings::{error::SettingsError, validated::ValidatedSettings, validator::SettingsValidator},
};
use async_trait::async_trait;
use batch_size::BatchSizeSetting;
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
use tracing::info;

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
pub mod validated;
pub mod validator;

#[async_trait]
pub trait MigrationSetting: Send + Sync {
    fn phase(&self) -> MigrationSettingsPhase;

    /// Check whether this setting should be applied in the given context.
    /// By default, all settings are applicable.
    fn can_apply(&self, _ctx: &ItemContext) -> bool {
        true
    }

    async fn apply(&mut self, _ctx: &mut ItemContext) -> Result<(), SettingsError> {
        Ok(())
    }
}

/// Validate and apply all migration settings.
pub async fn validate_and_apply(
    ctx: &mut ItemContext,
    settings: &Settings,
    is_dry_run: bool,
    dry_run_report: &Arc<Mutex<DryRunReport>>,
) -> Result<ValidatedSettings, SettingsError> {
    let validator = SettingsValidator::new(&ctx.source, &ctx.destination, is_dry_run);
    let validated_settings = validator.validate(settings).await?;

    let mut all_settings = collect_settings(ctx, dry_run_report, &validated_settings).await;
    for setting in all_settings.iter_mut() {
        if setting.can_apply(ctx) {
            let phase = setting.phase();
            info!("Applying setting: {:?}", phase);
            setting.apply(ctx).await?;
        }
    }

    Ok(validated_settings)
}

pub async fn collect_settings(
    ctx: &ItemContext,
    dry_run_report: &Arc<Mutex<DryRunReport>>,
    validated: &ValidatedSettings,
) -> Vec<Box<dyn MigrationSetting>> {
    let src = ctx.source.clone();
    let dest = ctx.destination.clone();
    let mapping = ctx.mapping.clone();

    let mut all_settings: Vec<Box<dyn MigrationSetting>> = Vec::new();

    if validated.batch_size() > 0 {
        all_settings.push(Box::new(BatchSizeSetting(validated.batch_size() as i64)));
    }

    all_settings.push(Box::new(CopyColumnsSetting(*validated.copy_columns())));

    if validated.ignore_constraints() {
        all_settings.push(Box::new(IgnoreConstraintsSettings(true)));
    }

    if validated.infer_schema() {
        let infer_schema_setting =
            InferSchemaSetting::new(&src, &dest, &mapping, validated, dry_run_report).await;
        all_settings.push(Box::new(infer_schema_setting));
    }

    if validated.create_missing_tables() {
        let missing_tables_setting =
            CreateMissingTablesSetting::new(&src, &dest, &mapping, validated, dry_run_report).await;
        all_settings.push(Box::new(missing_tables_setting));
    }

    if validated.create_missing_columns() {
        let missing_cols_setting =
            CreateMissingColumnsSetting::new(&src, &dest, &mapping, validated, dry_run_report)
                .await;
        all_settings.push(Box::new(missing_cols_setting));
    }

    // Settings are already created in phase order due to enum ordering
    all_settings.sort_by_key(|s| s.phase());

    all_settings
}
