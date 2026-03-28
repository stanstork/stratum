use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum HashAlgorithm {
    #[default]
    Sha256, // Hardware-accelerated via SHA-NI. ~3 GB/s.
    Blake3, // Faster on CPUs without SHA-NI. ~5 GB/s.
}
