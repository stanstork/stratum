use crate::error::StateStoreError;
use crate::models::{Checkpoint, CheckpointSummary, WalEntry};
use async_trait::async_trait;

#[async_trait]
pub trait StateStore: Send + Sync {
    async fn save_checkpoint(&self, cp: &Checkpoint) -> Result<(), StateStoreError>;
    async fn load_checkpoint(
        &self,
        run_id: &str,
        item_id: &str,
        part_id: &str,
    ) -> Result<Option<Checkpoint>, StateStoreError>;

    async fn last_checkpoint(
        &self,
        run_id: &str,
        item_id: &str,
        part_id: &str,
    ) -> Result<Option<CheckpointSummary>, StateStoreError> {
        Ok(self
            .load_checkpoint(run_id, item_id, part_id)
            .await?
            .map(CheckpointSummary::from))
    }

    async fn append_wal(&self, entry: &WalEntry) -> Result<(), StateStoreError>;
    async fn iter_wal(&self, run_id: &str) -> Result<Vec<WalEntry>, StateStoreError>;
}
