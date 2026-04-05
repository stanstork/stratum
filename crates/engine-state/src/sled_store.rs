use crate::error::StateStoreError;
use crate::merkle_store::MerkleStore;
use crate::models::{Checkpoint, CheckpointStage, RunState, WalEntry};
use crate::store::StateStore;
use async_trait::async_trait;
use model::integrity::receipt::VerificationReceipt;
use sled::transaction::{ConflictableTransactionError, TransactionError};
use std::path::Path;

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
        match self
            .db
            .get(key)
            .map_err(|e| StateStoreError::Storage(e.to_string()))?
        {
            Some(bytes) => {
                Ok(Some(bincode::deserialize(&bytes).map_err(|e| {
                    StateStoreError::Serialization(e.to_string())
                })?))
            }
            None => Ok(None),
        }
    }

    async fn append_wal(&self, entry: &WalEntry) -> Result<(), StateStoreError> {
        let seq = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let key = format!("wal:{}:{}", entry.run_id(), seq);
        let value =
            bincode::serialize(entry).map_err(|e| StateStoreError::Serialization(e.to_string()))?;

        self.db
            .insert(key, value)
            .map_err(|e| StateStoreError::Storage(e.to_string()))?;
        Ok(())
    }

    async fn iter_wal(&self, run_id: &str) -> Result<Vec<WalEntry>, StateStoreError> {
        let prefix = format!("wal:{}:", run_id);
        let mut entries = Vec::new();

        for item in self.db.scan_prefix(prefix) {
            let (_key, value) = item.map_err(|e| StateStoreError::Storage(e.to_string()))?;
            let entry: WalEntry = bincode::deserialize(&value)
                .map_err(|e| StateStoreError::Serialization(e.to_string()))?;
            entries.push(entry);
        }

        Ok(entries)
    }

    async fn save_run_state(&self, state: &RunState) -> Result<(), StateStoreError> {
        let key = format!("run:{}", state.run_id);
        let value =
            bincode::serialize(state).map_err(|e| StateStoreError::Serialization(e.to_string()))?;
        self.db
            .insert(key, value)
            .map_err(|e| StateStoreError::Storage(e.to_string()))?;
        Ok(())
    }

    async fn load_run_state(&self, run_id: &str) -> Result<Option<RunState>, StateStoreError> {
        let key = format!("run:{}", run_id);
        match self
            .db
            .get(key)
            .map_err(|e| StateStoreError::Storage(e.to_string()))?
        {
            Some(bytes) => {
                Ok(Some(bincode::deserialize(&bytes).map_err(|e| {
                    StateStoreError::Serialization(e.to_string())
                })?))
            }
            None => Ok(None),
        }
    }

    async fn list_runs(&self) -> Result<Vec<RunState>, StateStoreError> {
        let prefix = "run:";
        let mut runs = Vec::new();

        for item in self.db.scan_prefix(prefix) {
            let (_key, value) = item.map_err(|e| StateStoreError::Storage(e.to_string()))?;
            let run: RunState = bincode::deserialize(&value)
                .map_err(|e| StateStoreError::Serialization(e.to_string()))?;
            runs.push(run);
        }

        Ok(runs)
    }

    async fn delete_run(&self, run_id: &str) -> Result<(), StateStoreError> {
        // Delete run state
        self.db
            .remove(format!("run:{}", run_id))
            .map_err(|e| StateStoreError::Storage(e.to_string()))?;

        // Delete all checkpoints for this run
        for item in self.db.scan_prefix(format!("chk:{}:", run_id)) {
            let (key, _) = item.map_err(|e| StateStoreError::Storage(e.to_string()))?;
            self.db
                .remove(key)
                .map_err(|e| StateStoreError::Storage(e.to_string()))?;
        }

        // Delete all WAL entries for this run
        for item in self.db.scan_prefix(format!("wal:{}:", run_id)) {
            let (key, _) = item.map_err(|e| StateStoreError::Storage(e.to_string()))?;
            self.db
                .remove(key)
                .map_err(|e| StateStoreError::Storage(e.to_string()))?;
        }

        self.db
            .flush()
            .map_err(|e| StateStoreError::Storage(e.to_string()))?;

        Ok(())
    }
}

#[async_trait]
impl MerkleStore for SledStateStore {
    async fn save_receipt(&self, receipt: &VerificationReceipt) -> Result<(), StateStoreError> {
        let key = format!("receipt:{}:{}", receipt.pipeline_name, receipt.table_name);
        let value = serde_json::to_vec(receipt)
            .map_err(|e| StateStoreError::Serialization(e.to_string()))?;
        self.db
            .insert(key, value)
            .map_err(|e| StateStoreError::Storage(e.to_string()))?;
        Ok(())
    }

    async fn load_receipt(
        &self,
        pipeline_name: &str,
        table_name: &str,
    ) -> Result<Option<VerificationReceipt>, StateStoreError> {
        let key = format!("receipt:{}:{}", pipeline_name, table_name);
        match self
            .db
            .get(key)
            .map_err(|e| StateStoreError::Storage(e.to_string()))?
        {
            Some(bytes) => {
                Ok(Some(serde_json::from_slice(&bytes).map_err(|e| {
                    StateStoreError::Serialization(e.to_string())
                })?))
            }
            None => Ok(None),
        }
    }

    async fn list_receipts(&self) -> Result<Vec<VerificationReceipt>, StateStoreError> {
        let prefix = "receipt:";
        let mut receipts = Vec::new();
        for item in self.db.scan_prefix(prefix) {
            let (_key, value) = item.map_err(|e| StateStoreError::Storage(e.to_string()))?;
            let receipt: VerificationReceipt = serde_json::from_slice(&value)
                .map_err(|e| StateStoreError::Serialization(e.to_string()))?;
            receipts.push(receipt);
        }
        Ok(receipts)
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
