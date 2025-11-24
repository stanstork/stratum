use super::{MigrationSetting, phase::MigrationSettingsPhase};
use crate::settings::error::SettingsError;
use async_trait::async_trait;
use engine_core::context::item::ItemContext;
use tracing::info;

pub struct BatchSizeSetting(pub i64);

#[async_trait]
impl MigrationSetting for BatchSizeSetting {
    fn phase(&self) -> MigrationSettingsPhase {
        MigrationSettingsPhase::BatchSize
    }

    async fn apply(&mut self, ctx: &mut ItemContext) -> Result<(), SettingsError> {
        let mut settings = ctx.settings.lock().await;
        settings.set_batch_size(self.0 as usize);
        info!("Batch size setting applied");
        Ok(())
    }
}
