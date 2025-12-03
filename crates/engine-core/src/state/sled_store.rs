use crate::state::{
    StateStore,
    models::{Checkpoint, WalEntry},
};
use async_trait::async_trait;
use sled::transaction::{ConflictableTransactionError, TransactionError};
use std::{error::Error, path::Path};

pub struct SledStateStore {
    db: sled::Db,
}

impl SledStateStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, sled::Error> {
        let db = sled::open(path)?;
        Ok(Self { db })
    }

    /// Helper to generate consistent keys for checkpoints
    #[inline]
    fn chk_key(run_id: &str, item_id: &str, part_id: &str) -> String {
        format!("chk:{}:{}:{}", run_id, item_id, part_id)
    }

    /// Ranks the stage for comparison logic.
    /// Order: Read (1) < Write (2) < Committed (3)
    fn stage_rank(stage: &str) -> u8 {
        match stage {
            "read" => 1,
            "write" => 2,
            "committed" => 3,
            _ => 0,
        }
    }
}

#[async_trait]
impl StateStore for SledStateStore {
    async fn save_checkpoint(&self, cp: &Checkpoint) -> Result<(), Box<dyn Error + Send + Sync>> {
        let key = Self::chk_key(&cp.run_id, &cp.item_id, &cp.part_id);
        let new_bytes = bincode::serialize(cp)?;

        // Use a transaction to ensure atomic "check-then-set" logic.
        // This prevents race conditions where multiple threads/consumers
        // might try to update the status simultaneously.
        let result = self
            .db
            .transaction::<_, _, Box<dyn Error + Send + Sync>>(|tx_db| {
                // Check if existing state prevents this update
                if let Some(existing_bytes) = tx_db.get(&key)? {
                    // We must map deserialization errors to Abort to stop the transaction safely
                    let existing: Checkpoint = bincode::deserialize(&existing_bytes)
                        .map_err(|e| ConflictableTransactionError::Abort(e.into()))?;

                    let is_same_batch = existing.batch_id == cp.batch_id;
                    let is_committed = existing.stage == "committed";

                    // Logic:
                    // If batch matches, only update if new stage >= old stage (e.g., read -> write).
                    // If batch differs, only update if the previous batch was fully committed.
                    let should_update = if is_same_batch {
                        Self::stage_rank(&cp.stage) >= Self::stage_rank(&existing.stage)
                    } else {
                        is_committed
                    };

                    if !should_update {
                        // Intentionally skip update, not an error.
                        return Ok(());
                    }
                }

                // 2. Apply update
                tx_db.insert(&*key, new_bytes.as_slice())?;
                Ok(())
            });

        match result {
            Ok(_) => Ok(()),
            Err(TransactionError::Abort(e)) => Err(e),
            Err(TransactionError::Storage(e)) => Err(Box::new(e)),
        }
    }

    async fn load_checkpoint(
        &self,
        run_id: &str,
        item_id: &str,
        part_id: &str,
    ) -> Result<Option<Checkpoint>, Box<dyn Error + Send + Sync>> {
        let key = Self::chk_key(run_id, item_id, part_id);
        match self.db.get(key)? {
            Some(bytes) => Ok(Some(bincode::deserialize(&bytes)?)),
            None => Ok(None),
        }
    }

    async fn append_wal(&self, entry: &WalEntry) -> Result<(), Box<dyn Error + Send + Sync>> {
        // timestamp_nanos_opt is safe, but we default to 0 just in case of specialized OS issues
        let seq = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let key = format!("wal:{}:{}", entry.run_id(), seq);
        let value = bincode::serialize(entry)?;

        self.db.insert(key, value)?;
        Ok(())
    }

    async fn iter_wal(&self, run_id: &str) -> Result<Vec<WalEntry>, Box<dyn Error + Send + Sync>> {
        let prefix = format!("wal:{}:", run_id);
        let mut entries = Vec::new();

        // Sled iterators handle errors internally
        for item in self.db.scan_prefix(prefix) {
            let (_key, value) = item?;
            let entry: WalEntry = bincode::deserialize(&value)?;
            entries.push(entry);
        }

        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use model::pagination::cursor::Cursor;
    use tempfile::tempdir;

    // Helper to create dummy checkpoints
    fn mk_cp(stage: &str, batch: &str, cursor: Cursor) -> Checkpoint {
        Checkpoint {
            run_id: "run".into(),
            item_id: "item".into(),
            part_id: "part".into(),
            stage: stage.to_string(),
            src_offset: cursor,
            pending_offset: None,
            batch_id: batch.to_string(),
            rows_done: 0,
            updated_at: chrono::Utc::now(),
        }
    }
    #[tokio::test]
    async fn keeps_uncommitted_write_over_future_read() {
        let dir = tempdir().unwrap();
        let store = SledStateStore::open(dir.path()).unwrap();

        store
            .save_checkpoint(&mk_cp("write", "batch-1", Cursor::None))
            .await
            .unwrap();

        // Producer races ahead with a read for the next batch, but we should not
        // advance past the uncommitted write.
        store
            .save_checkpoint(&mk_cp("read", "batch-2", Cursor::Default { offset: 1 }))
            .await
            .unwrap();

        let cp = store
            .load_checkpoint("run", "item", "part")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cp.stage, "write");
        assert_eq!(cp.batch_id, "batch-1");
    }

    #[tokio::test]
    async fn advances_after_commit() {
        let dir = tempdir().unwrap();
        let store = SledStateStore::open(dir.path()).unwrap();

        // 1. Save a "committed" state
        store
            .save_checkpoint(&mk_cp("committed", "batch-1", Cursor::None))
            .await
            .unwrap();

        // 2. Save next batch "read"
        store
            .save_checkpoint(&mk_cp("read", "batch-2", Cursor::Default { offset: 1 }))
            .await
            .unwrap();

        let cp = store
            .load_checkpoint("run", "item", "part")
            .await
            .unwrap()
            .unwrap();

        // Should successfully advance
        assert_eq!(cp.stage, "read");
        assert_eq!(cp.batch_id, "batch-2");
    }
}
