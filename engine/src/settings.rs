use crate::state::MigrationState;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex;

#[async_trait]
pub trait MigrationSetting {
    async fn apply(&self, state: Arc<Mutex<MigrationState>>);
}

pub struct InferSchemaSetting;
pub struct BatchSizeSetting(pub i64);

#[async_trait]
impl MigrationSetting for InferSchemaSetting {
    async fn apply(&self, state: Arc<Mutex<MigrationState>>) {
        let mut state = state.lock().await;
        state.infer_schema = true;
        println!("Infer schema setting applied");
    }
}

#[async_trait]
impl MigrationSetting for BatchSizeSetting {
    async fn apply(&self, state: Arc<Mutex<MigrationState>>) {
        let mut state = state.lock().await;
        state.batch_size = self.0 as usize;
        println!("Batch size setting applied");
    }
}
