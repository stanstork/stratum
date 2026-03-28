use crate::error::StateStoreError;
use async_trait::async_trait;
use model::integrity::receipt::VerificationReceipt;

/// Persistence layer for integrity verification data.
#[async_trait]
pub trait MerkleStore: Send + Sync {
    /// Persist a `VerificationReceipt` after a pipeline completes with --integrity.
    /// Key: `receipt:{pipeline_name}:{table_name}` - stable across runs so that
    /// each new `apply --integrity` overwrites the previous receipt.
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
