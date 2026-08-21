use crate::error::StateStoreError;
use crate::store::SledStateStore;
use crate::store::{to_ser, to_storage};
use async_trait::async_trait;
use model::integrity::receipt::VerificationReceipt;

/// Persistence for verification receipts.
#[async_trait]
pub trait MerkleStore: Send + Sync {
    /// Persist a `VerificationReceipt` after a pipeline completes with --integrity.
    async fn save_receipt(&self, receipt: &VerificationReceipt) -> Result<(), StateStoreError>;

    /// Load the receipt written by the most recent `apply --integrity` run
    /// for this pipeline+table pair.
    async fn load_receipt(
        &self,
        pipeline_name: &str,
        table_name: &str,
    ) -> Result<Option<VerificationReceipt>, StateStoreError>;

    /// List all receipts across all pipelines and tables.
    async fn list_receipts(&self) -> Result<Vec<VerificationReceipt>, StateStoreError>;
}

#[async_trait]
impl MerkleStore for SledStateStore {
    async fn save_receipt(&self, receipt: &VerificationReceipt) -> Result<(), StateStoreError> {
        let key = format!("receipt:{}:{}", receipt.pipeline_name, receipt.table_name);
        let value = serde_json::to_vec(receipt).map_err(to_ser)?;

        self.db.insert(key, value).map_err(to_storage)?;
        Ok(())
    }

    async fn load_receipt(
        &self,
        pipeline_name: &str,
        table_name: &str,
    ) -> Result<Option<VerificationReceipt>, StateStoreError> {
        let key = format!("receipt:{}:{}", pipeline_name, table_name);
        self.db
            .get(key)
            .map_err(to_storage)?
            .map(|bytes| serde_json::from_slice(&bytes).map_err(to_ser))
            .transpose()
    }

    async fn list_receipts(&self) -> Result<Vec<VerificationReceipt>, StateStoreError> {
        self.db
            .scan_prefix("receipt:")
            .map(|item| {
                let (_, value) = item.map_err(to_storage)?;
                serde_json::from_slice(&value).map_err(to_ser)
            })
            .collect()
    }
}
