use super::{phase::MigrationSettingsPhase, MigrationSetting};
use crate::{context::item::ItemContext, error::MigrationError};
use async_trait::async_trait;
use tracing::info;

pub struct IgnoreConstraintsSettings(pub bool);

#[async_trait]
impl MigrationSetting for IgnoreConstraintsSettings {
    fn phase(&self) -> MigrationSettingsPhase {
        MigrationSettingsPhase::IgnoreConstraints
    }

    async fn apply(&mut self, ctx: &mut ItemContext) -> Result<(), MigrationError> {
        let mut state = ctx.state.lock().await;
        state.ignore_constraints = self.0;
        info!("Ignore constraints setting applied");
        Ok(())
    }
}
