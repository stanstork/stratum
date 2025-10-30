use crate::state::models::{Checkpoint, WallEntry};
use async_trait::async_trait;
use std::error::Error;

pub mod buffer;
mod models;
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
    async fn append_wal(&self, entry: &WallEntry) -> Result<(), Box<dyn Error + Send + Sync>>;
    async fn iter_wal(&self, run_id: &str) -> Result<Vec<WallEntry>, Box<dyn Error + Send + Sync>>;
}
