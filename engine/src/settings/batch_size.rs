use super::{phase::MigrationSettingsPhase, MigrationSetting};
use crate::{context::item::ItemContext, error::MigrationError};
use async_trait::async_trait;
use tracing::info;

pub struct BatchSizeSetting(pub i64);

#[async_trait]
impl MigrationSetting for BatchSizeSetting {
    fn phase(&self) -> MigrationSettingsPhase {
        MigrationSettingsPhase::BatchSize
    }

    async fn apply(&mut self, ctx: &mut ItemContext) -> Result<(), MigrationError> {
        let mut state = ctx.state.lock().await;
        state.set_batch_size(self.0 as usize);
        info!("Batch size setting applied");
        Ok(())
    }
}
