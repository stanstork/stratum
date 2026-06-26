use wasmtime::{StoreLimits, StoreLimitsBuilder};

#[derive(Debug, Clone)]
pub struct ResourceLimits {
    /// Maximum guest memory in bytes. Default: 64MB for transform/filter, 128MB for source/sink.
    pub max_memory_bytes: usize,
    /// Wasmtime fuel units per call. Default: 1_000_000.
    /// Each WASM instruction consumes ~1 fuel unit.
    pub max_execution_fuel: u64,
    /// Maximum output size in bytes. Default: 1MB for transform/filter, 16MB for source/sink.
    pub max_output_bytes: usize,
    /// Wall-clock timeout per call in ms. Default: 1000 for transform/filter, 30000 for source/sink.
    pub timeout_ms: u64,
}

impl ResourceLimits {
    pub fn for_row_plugins() -> Self {
        Self {
            max_memory_bytes: 64 * 1024 * 1024, // 64 MB
            max_execution_fuel: 1_000_000,
            max_output_bytes: 1024 * 1024, // 1 MB
            timeout_ms: 1_000,
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
    /// Allow persistent key-value store. Default: false.
    pub key_value_store: bool,
    /// Allow custom metrics emission. Default: false.
    pub metrics: bool,
}

impl Default for HostCapabilities {
    fn default() -> Self {
        Self {
            logging: true,
            http_client: false,
            key_value_store: false,
            metrics: false,
        }
    }
}
