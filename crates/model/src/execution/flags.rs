/// Controls whether integrity hashing runs during `apply` and at what depth.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum IntegrityMode {
    #[default]
    Off,
    /// Compute a Merkle root per batch and write a `VerificationReceipt` to sled.
    BatchHashes,
    /// `BatchHashes` plus individual row hashes stored in the receipt.
    /// Enables row-level divergence reporting during verify (exact row index).
    /// Uses ~32 bytes per migrated row of additional sled storage.
    FullHashes,
}

impl IntegrityMode {
    pub fn new(integrity: bool, full_integrity: bool) -> Self {
        if full_integrity {
            IntegrityMode::FullHashes
        } else if integrity {
            IntegrityMode::BatchHashes
        } else {
            IntegrityMode::Off
        }
    }

    pub fn is_enabled(self) -> bool {
        !matches!(self, Self::Off)
    }

    pub fn store_row_hashes(self) -> bool {
        matches!(self, Self::FullHashes)
    }
}

/// Runtime execution flags passed from CLI arguments down through the executor.
#[derive(Debug, Clone, Copy, Default)]
pub struct ExecutionFlags {
    /// Perform all planning and validation but apply no changes to the destination.
    pub dry_run: bool,
    /// Integrity hashing mode.
    pub integrity: IntegrityMode,
}

impl ExecutionFlags {
    pub fn new(dry_run: bool, integrity: IntegrityMode) -> Self {
        Self { dry_run, integrity }
    }
}
