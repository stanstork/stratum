use std::path::PathBuf;
use wasmtime::{StoreLimits, StoreLimitsBuilder};

#[derive(Debug, Clone)]
pub struct ResourceLimits {
    /// Maximum guest memory in bytes. 128 MB for both row and IO plugins.
    pub max_memory_bytes: usize,
    /// Wasmtime fuel units, ~1 per WASM instruction. For row plugins this is a
    /// per-row rate scaled by the batch size, not a flat per-call budget:
    /// 1_000_000 per row. IO plugins get a flat 100_000_000 per call.
    pub max_execution_fuel: u64,
    /// Maximum output size in bytes: a 1 MB per-row rate for row plugins,
    /// a flat 16 MB for IO plugins.
    pub max_output_bytes: usize,
    /// Wall-clock budget in ms: a 1000 per-row rate for row plugins, a flat
    /// 30000 for IO plugins.
    pub timeout_ms: u64,
}

impl ResourceLimits {
    pub fn for_row_plugins() -> Self {
        Self {
            max_memory_bytes: 128 * 1024 * 1024, // 128 MB
            max_execution_fuel: 1_000_000,       // per-row fuel rate
            max_output_bytes: 1024 * 1024,       // per-row output rate (1 MB)
            timeout_ms: 1_000,                   // per-row wall-clock rate
        }
    }

    pub fn for_io_plugins() -> Self {
        Self {
            max_memory_bytes: 128 * 1024 * 1024, // 128 MB
            max_execution_fuel: 100_000_000,
            max_output_bytes: 16 * 1024 * 1024, // 16 MB
            timeout_ms: 30_000,
        }
    }

    pub(crate) fn to_store_limits(&self) -> StoreLimits {
        StoreLimitsBuilder::new()
            .memory_size(self.max_memory_bytes)
            .build()
    }
}

#[derive(Debug, Clone)]
pub struct HostCapabilities {
    /// Allow guest logging via log_* host functions. Default: true.
    pub logging: bool,
    /// Allow outbound HTTP requests. Default: false.
    pub http_client: bool,
    /// Optional host allowlist for HTTP.
    pub http_allowed_hosts: Vec<String>,
    /// Allow instance-scoped scratch key-value store. Default: false.
    pub key_value_store: bool,
    /// Allow custom metrics emission. Default: false.
    pub metrics: bool,
    /// Environment variables (name, value) exposed to the guest via WASI.
    pub env: Vec<(String, String)>,
    /// Host directories preopened read-only for the guest (WASI).
    pub fs_read: Vec<PathBuf>,
    /// Host directories preopened read-write for the guest (WASI).
    pub fs_write: Vec<PathBuf>,
}

impl Default for HostCapabilities {
    fn default() -> Self {
        Self {
            logging: true,
            http_client: false,
            http_allowed_hosts: Vec::new(),
            key_value_store: false,
            metrics: false,
            env: Vec::new(),
            fs_read: Vec::new(),
            fs_write: Vec::new(),
        }
    }
}
