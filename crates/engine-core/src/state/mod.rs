use crate::state::models::{Checkpoint, CheckpointSummary, WalEntry};
use async_trait::async_trait;
use std::error::Error;

pub mod buffer;
pub mod models;
pub mod sled_store;

#[async_trait]
pub trait StateStore: Send + Sync {
    async fn save_checkpoint(&self, cp: &Checkpoint) -> Result<(), Box<dyn Error + Send + Sync>>;
    async fn load_checkpoint(
        &self,
        run_id: &str,
        item_id: &str,
        part_id: &str,
    ) -> Result<Option<Checkpoint>, Box<dyn Error + Send + Sync>>;

    async fn last_checkpoint(
        &self,
        run_id: &str,
        item_id: &str,
        part_id: &str,
    ) -> Result<Option<CheckpointSummary>, Box<dyn Error + Send + Sync>> {
        Ok(self
            .load_checkpoint(run_id, item_id, part_id)
            .await?
            .map(CheckpointSummary::from))
    }

    async fn append_wal(&self, entry: &WalEntry) -> Result<(), Box<dyn Error + Send + Sync>>;
    async fn iter_wal(&self, run_id: &str) -> Result<Vec<WalEntry>, Box<dyn Error + Send + Sync>>;
}
