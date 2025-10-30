use super::{MigrationSetting, phase::MigrationSettingsPhase};
use crate::settings::error::SettingsError;
use async_trait::async_trait;
use engine_core::context::item::ItemContext;
use tracing::info;

pub struct IgnoreConstraintsSettings(pub bool);

#[async_trait]
impl MigrationSetting for IgnoreConstraintsSettings {
    fn phase(&self) -> MigrationSettingsPhase {
        MigrationSettingsPhase::IgnoreConstraints
    }

    async fn apply(&mut self, ctx: &mut ItemContext) -> Result<(), SettingsError> {
        let mut state = ctx.state.lock().await;
        state.set_ignore_constraints(self.0);
        info!("Ignore constraints setting applied");
        Ok(())
    }
}
