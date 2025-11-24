use engine_config::{
    report::dry_run::DryRunReport,
    settings::{collect_settings, error::SettingsError},
};
use engine_core::context::item::ItemContext;
use futures::lock::Mutex;
use smql_syntax::ast::setting::Settings;
use std::sync::Arc;
use tracing::info;

pub async fn apply_all(
    ctx: &mut ItemContext,
    settings: &Settings,
    dry_run_report: &Arc<Mutex<DryRunReport>>,
) -> Result<(), SettingsError> {
    info!("Applying migration settings");

    let mut settings = collect_settings(settings, ctx, dry_run_report).await;
    for setting in settings.iter_mut() {
        if setting.can_apply(ctx) {
            setting.apply(ctx).await?;
        }
    }

    Ok(())
}
