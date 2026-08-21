use crate::SledStateStore;
use crate::error::StateStoreError;
use crate::models::{Checkpoint, CheckpointStage, CheckpointSummary, RunState, WalEntry};
use crate::store::{to_ser, to_storage};
use async_trait::async_trait;
use sled::transaction::{ConflictableTransactionError, TransactionError};

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

    async fn total_rows_done(&self, run_id: &str, item_id: &str) -> Result<u64, StateStoreError>;

    async fn append_wal(&self, entry: &WalEntry) -> Result<(), StateStoreError>;
    async fn iter_wal(&self, run_id: &str) -> Result<Vec<WalEntry>, StateStoreError>;

    async fn save_run_state(&self, state: &RunState) -> Result<(), StateStoreError>;
    async fn load_run_state(&self, run_id: &str) -> Result<Option<RunState>, StateStoreError>;
    async fn list_runs(&self) -> Result<Vec<RunState>, StateStoreError>;

    /// Delete all state for a given run: run state, checkpoints, and WAL entries.
    async fn delete_run(&self, run_id: &str) -> Result<(), StateStoreError>;
}

#[async_trait]
impl StateStore for SledStateStore {
    async fn save_checkpoint(&self, cp: &Checkpoint) -> Result<(), StateStoreError> {
        let key = Self::chk_key(&cp.run_id, &cp.item_id, &cp.part_id);
        let new_bytes =
            bincode::serialize(cp).map_err(|e| StateStoreError::Serialization(e.to_string()))?;

        let result = self.db.transaction::<_, _, StateStoreError>(|tx_db| {
            if let Some(existing_bytes) = tx_db.get(&key).map_err(|e| {
                ConflictableTransactionError::Abort(StateStoreError::Storage(e.to_string()))
            })? {
                let existing: Checkpoint = bincode::deserialize(&existing_bytes).map_err(|e| {
                    ConflictableTransactionError::Abort(StateStoreError::Serialization(
                        e.to_string(),
                    ))
                })?;

                let is_same_batch = existing.batch_id == cp.batch_id;
                let is_committed = existing.stage == CheckpointStage::Committed;

                let should_update = if is_same_batch {
                    cp.stage >= existing.stage
                } else {
                    is_committed
                };

                if !should_update {
                    return Ok(());
                }
            }

            tx_db.insert(&*key, new_bytes.as_slice()).map_err(|e| {
                ConflictableTransactionError::Abort(StateStoreError::Storage(e.to_string()))
            })?;
            Ok(())
        });

        match result {
            Ok(_) => Ok(()),
            Err(TransactionError::Abort(e)) => Err(e),
            Err(TransactionError::Storage(e)) => Err(StateStoreError::Storage(e.to_string())),
        }
    }

    async fn load_checkpoint(
        &self,
        run_id: &str,
        item_id: &str,
        part_id: &str,
    ) -> Result<Option<Checkpoint>, StateStoreError> {
        let key = Self::chk_key(run_id, item_id, part_id);
        self.db
            .get(key)
            .map_err(to_storage)?
            .map(|bytes| bincode::deserialize(&bytes).map_err(to_ser))
            .transpose()
    }

    async fn total_rows_done(&self, run_id: &str, item_id: &str) -> Result<u64, StateStoreError> {
        // Sum `rows_done` over every part under this item (chk:run:item:part-*).
        self.db
            .scan_prefix(format!("chk:{}:{}:", run_id, item_id))
            .try_fold(0u64, |acc, entry| {
                let (_, value) = entry.map_err(to_storage)?;
                let cp: Checkpoint = bincode::deserialize(&value).map_err(to_ser)?;
                Ok(acc + cp.rows_done)
            })
    }

    async fn append_wal(&self, entry: &WalEntry) -> Result<(), StateStoreError> {
        let seq = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let key = format!("wal:{}:{}", entry.run_id(), seq);
        let value = bincode::serialize(entry).map_err(to_ser)?;

        self.db.insert(key, value).map_err(to_storage)?;
        Ok(())
    }

    async fn iter_wal(&self, run_id: &str) -> Result<Vec<WalEntry>, StateStoreError> {
        self.db
            .scan_prefix(format!("wal:{}:", run_id))
            .map(|item| {
                let (_, value) = item.map_err(to_storage)?;
                bincode::deserialize(&value).map_err(to_ser)
            })
            .collect()
    }

    async fn save_run_state(&self, state: &RunState) -> Result<(), StateStoreError> {
        let key = format!("run:{}", state.run_id);
        let value = bincode::serialize(state).map_err(to_ser)?;

        self.db.insert(key, value).map_err(to_storage)?;
        Ok(())
    }

    async fn load_run_state(&self, run_id: &str) -> Result<Option<RunState>, StateStoreError> {
        let key = format!("run:{}", run_id);
        self.db
            .get(key)
            .map_err(to_storage)?
            .map(|bytes| bincode::deserialize(&bytes).map_err(to_ser))
            .transpose()
    }

    async fn list_runs(&self) -> Result<Vec<RunState>, StateStoreError> {
        self.db
            .scan_prefix("run:")
            .map(|item| {
                let (_, value) = item.map_err(to_storage)?;
                bincode::deserialize(&value).map_err(to_ser)
            })
            .collect()
    }

    async fn delete_run(&self, run_id: &str) -> Result<(), StateStoreError> {
        // Collect deletions atomically in a single batch to drastically reduce runtime I/O overhead.
        let mut batch = sled::Batch::default();

        batch.remove(format!("run:{}", run_id).as_bytes());

        for item in self.db.scan_prefix(format!("chk:{}:", run_id)) {
            let (key, _) = item.map_err(to_storage)?;
            batch.remove(key);
        }

        for item in self.db.scan_prefix(format!("wal:{}:", run_id)) {
            let (key, _) = item.map_err(to_storage)?;
            batch.remove(key);
        }

        self.db.apply_batch(batch).map_err(to_storage)?;
        self.db.flush().map_err(to_storage)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use model::pagination::cursor::Cursor;
    use tempfile::tempdir;

    // Helper to create dummy checkpoints
    fn mk_cp(stage: CheckpointStage, batch: &str, cursor: Cursor) -> Checkpoint {
        Checkpoint {
            run_id: "run".into(),
            item_id: "item".into(),
            part_id: "part".into(),
            stage,
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
            .save_checkpoint(&mk_cp(CheckpointStage::Write, "batch-1", Cursor::None))
            .await
            .unwrap();

        store
            .save_checkpoint(&mk_cp(
                CheckpointStage::Read,
                "batch-2",
                Cursor::Default { offset: 1 },
            ))
            .await
            .unwrap();

        let cp = store
            .load_checkpoint("run", "item", "part")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cp.stage, CheckpointStage::Write);
        assert_eq!(cp.batch_id, "batch-1");
    }

    #[tokio::test]
    async fn advances_after_commit() {
        let dir = tempdir().unwrap();
        let store = SledStateStore::open(dir.path()).unwrap();

        store
            .save_checkpoint(&mk_cp(CheckpointStage::Committed, "batch-1", Cursor::None))
            .await
            .unwrap();

        store
            .save_checkpoint(&mk_cp(
                CheckpointStage::Read,
                "batch-2",
                Cursor::Default { offset: 1 },
            ))
            .await
            .unwrap();

        let cp = store
            .load_checkpoint("run", "item", "part")
            .await
            .unwrap()
            .unwrap();

        assert_eq!(cp.stage, CheckpointStage::Read);
        assert_eq!(cp.batch_id, "batch-2");
    }
}
