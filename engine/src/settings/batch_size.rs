use super::{phase::MigrationSettingsPhase, MigrationSetting};
use crate::context::item::ItemContext;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

pub struct BatchSizeSetting(pub i64);

#[async_trait]
impl MigrationSetting for BatchSizeSetting {
    fn phase(&self) -> MigrationSettingsPhase {
        MigrationSettingsPhase::BatchSize
    }

    async fn apply(&self, ctx: Arc<Mutex<ItemContext>>) -> Result<(), Box<dyn std::error::Error>> {
        let context = ctx.lock().await;
        let mut state = context.state.lock().await;
        state.batch_size = self.0 as usize;
        info!("Batch size setting applied");
        Ok(())
    }
}
