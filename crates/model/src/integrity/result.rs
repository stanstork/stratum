use crate::integrity::receipt::VerificationReceipt;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum VerificationResult {
    Match {
        receipt: VerificationReceipt,
        duration_ms: u64,
    },
    Mismatch {
        receipt: VerificationReceipt,
        actual_root: [u8; 32],
        divergent_batches: Vec<DivergentBatch>,
        duration_ms: u64,
    },
    /// No receipt found in Sled - pipeline was run without --integrity.
    NoPriorRun { pipeline_name: String },
}

/// A batch whose recomputed root does not match the stored root.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DivergentBatch {
    pub batch_index: u64,
    pub expected_root: [u8; 32],
    pub actual_root: [u8; 32],
    /// Inclusive start row index of this batch (batch_index * batch_size).
    pub row_start: u64,
    /// Exclusive end row index (row_start + batch row_count).
    pub row_end: u64,
    /// Row-level detail, populated only when the receipt was written with `--full-integrity`.
    #[serde(default)]
    pub divergent_rows: Vec<DivergentRow>,
}

/// A single row whose hash differs between the receipt and the destination.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DivergentRow {
    /// Zero-based row index across the whole table (not within the batch).
    pub row_index: u64,
    /// Hash stored in the receipt at migration time.
    pub expected_hash: [u8; 32],
    /// Hash recomputed from the current destination row.
    pub actual_hash: [u8; 32],
}
