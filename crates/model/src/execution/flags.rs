use serde::{Deserialize, Serialize};

/// Controls whether integrity hashing runs during `apply`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum IntegrityMode {
    #[default]
    Off,
    On,
}

impl IntegrityMode {
    pub fn new(integrity: bool) -> Self {
        if integrity {
            IntegrityMode::On
        } else {
            IntegrityMode::Off
        }
    }

    pub fn is_enabled(self) -> bool {
        matches!(self, Self::On)
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
