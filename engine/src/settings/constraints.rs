use super::{phase::MigrationSettingsPhase, MigrationSetting};
use crate::context::MigrationContext;
use async_trait::async_trait;
use smql::plan::MigrationPlan;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

pub struct IgnoreConstraintsSettings(pub bool);

#[async_trait]
impl MigrationSetting for IgnoreConstraintsSettings {
    fn phase(&self) -> MigrationSettingsPhase {
        MigrationSettingsPhase::IgnoreConstraints
    }

    async fn apply(
        &self,
        _plan: &MigrationPlan,
        context: Arc<Mutex<MigrationContext>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let context = context.lock().await;
        let mut state = context.state.lock().await;
        state.ignore_constraints = self.0;
        info!("Ignore constraints setting applied");
        Ok(())
    }
}
