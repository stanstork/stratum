use crate::state::{
    StateStore,
    models::{Checkpoint, WallEntry},
};
use async_trait::async_trait;
use std::{error::Error, path::Path};

pub struct SledStateStore {
    db: sled::Db,
}

impl SledStateStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, sled::Error> {
        let db = sled::open(path)?;
        Ok(Self { db })
    }
}

#[async_trait]
impl StateStore for SledStateStore {
    async fn save_checkpoint(&self, cp: &Checkpoint) -> Result<(), Box<dyn Error + Send + Sync>> {
        let key = format!("chk:{}:{}:{}", cp.run_id, cp.item_id, cp.part_id);
        let value = bincode::serialize(cp)?;
        self.db.insert(key, value)?;
        self.db.flush_async().await?;
        Ok(())
    }

    async fn load_checkpoint(
        &self,
        run_id: &str,
        item_id: &str,
        part_id: &str,
    ) -> Result<Option<Checkpoint>, Box<dyn Error + Send + Sync>> {
        let key = format!("chk:{}:{}:{}", run_id, item_id, part_id);
        Ok(match self.db.get(key)? {
            Some(bytes) => Some(bincode::deserialize(&bytes)?),
            None => None,
        })
    }

    async fn append_wal(&self, entry: &WallEntry) -> Result<(), Box<dyn Error + Send + Sync>> {
        let seq = chrono::Utc::now()
            .timestamp_nanos_opt()
            .ok_or("Timestamp overflow")?;
        let key = format!("wal:{}:{}", entry.run_id(), seq);
        let value = bincode::serialize(entry)?;
        self.db.insert(key, value)?;
        Ok(())
    }

    async fn iter_wal(&self, run_id: &str) -> Result<Vec<WallEntry>, Box<dyn Error + Send + Sync>> {
        let prefix = format!("wal:{}:", run_id);
        let mut entries = Vec::new();

        for item in self.db.scan_prefix(prefix) {
            let (_key, value) = item?;
            let entry: WallEntry = bincode::deserialize(&value)?;
            entries.push(entry);
        }

        Ok(entries)
    }
}
