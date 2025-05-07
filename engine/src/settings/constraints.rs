use super::{phase::MigrationSettingsPhase, MigrationSetting};
use crate::context::item::ItemContext;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

pub struct IgnoreConstraintsSettings(pub bool);

#[async_trait]
impl MigrationSetting for IgnoreConstraintsSettings {
    fn phase(&self) -> MigrationSettingsPhase {
        MigrationSettingsPhase::IgnoreConstraints
    }

    async fn apply(&self, ctx: Arc<Mutex<ItemContext>>) -> Result<(), Box<dyn std::error::Error>> {
        let context = ctx.lock().await;
        let mut state = context.state.lock().await;
        state.ignore_constraints = self.0;
        info!("Ignore constraints setting applied");
        Ok(())
    }
}
