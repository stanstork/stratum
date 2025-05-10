use super::{phase::MigrationSettingsPhase, MigrationSetting};
use crate::{context::item::ItemContext, error::MigrationError};
use async_trait::async_trait;
use tracing::info;

pub struct CascadeSchemaSetting(pub bool);

#[async_trait]
impl MigrationSetting for CascadeSchemaSetting {
    fn phase(&self) -> MigrationSettingsPhase {
        MigrationSettingsPhase::CascadeSchema
    }

    async fn apply(&self, ctx: &mut ItemContext) -> Result<(), MigrationError> {
        let mut state = ctx.state.lock().await;
        state.ignore_constraints = self.0;
        info!("Cascade schema setting applied");
        Ok(())
    }
}
