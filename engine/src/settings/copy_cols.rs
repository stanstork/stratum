use super::{phase::MigrationSettingsPhase, MigrationSetting};
use crate::{context::item::ItemContext, error::MigrationError};
use async_trait::async_trait;
use smql::statements::setting::CopyColumns;
use tracing::info;

pub struct CopyColumnsSetting(pub CopyColumns);

#[async_trait]
impl MigrationSetting for CopyColumnsSetting {
    fn phase(&self) -> MigrationSettingsPhase {
        MigrationSettingsPhase::BatchSize
    }

    async fn apply(&self, ctx: &mut ItemContext) -> Result<(), MigrationError> {
        let mut state = ctx.state.lock().await;
        state.copy_columns = self.0.clone();
        info!("Batch size setting applied");
        Ok(())
    }
}
