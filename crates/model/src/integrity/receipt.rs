use crate::integrity::algorithm::HashAlgorithm;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Written to Sled when a pipeline completes. Loaded by engine-verify.
/// Self-contained: carries everything needed to reproduce the hashing scheme.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerificationReceipt {
    pub run_id: String,
    pub pipeline_name: String,
    pub table_name: String,
    /// Root of the Merkle tree over all batch roots.
    pub table_root: [u8; 32],
    /// One subtree root per batch, ordered by batch_index.
    pub batch_roots: Vec<[u8; 32]>,
    /// Lexicographically sorted destination column names.
    /// Used to reproduce the canonical encoding on the verify path.
    pub column_order: Vec<String>,
    /// Rows that were hashed and written (excludes DLQ skips).
    pub total_rows: u64,
    /// Rows sent to DLQ - not present in destination.
    /// Allows verify to distinguish expected absences from data loss.
    pub skipped_rows: u64,
    /// Rows written per batch for this specific table.
    pub rows_per_batch: Vec<u64>,
    /// When true the verifier must sort row hashes before building the Merkle
    /// tree. Set for cascade tables whose rows arrive in non-PK order during
    /// migration. Sorting makes the hash order-independent so it matches the
    /// PK-ordered read during verify.
    pub sorted_hashes: bool,
    pub algorithm: HashAlgorithm,
    pub created_at: DateTime<Utc>,
    /// Individual row hashes in receipt order, one entry per migrated row.
    /// Enables row-level divergence detection during `verify`.
    pub row_hashes: Option<Vec<[u8; 32]>>,
}
