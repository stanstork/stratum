use super::MigrationSetting;
use crate::context::MigrationContext;
use async_trait::async_trait;
use smql::plan::MigrationPlan;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

pub struct BatchSizeSetting(pub i64);

#[async_trait]
impl MigrationSetting for BatchSizeSetting {
    async fn apply(
        &self,
        _plan: &MigrationPlan,
        context: Arc<Mutex<MigrationContext>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let context = context.lock().await;
        let mut state = context.state.lock().await;
        state.batch_size = self.0 as usize;
        info!("Batch size setting applied");
        Ok(())
    }
}
