use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum WasmError {
    #[error("Failed to serialize plugin input: {0}")]
    SerializationError(String),

    #[error("Failed to deserialize plugin output: {0}")]
    DeserializationError(String),

    #[error("Plugin '{plugin}' returned error: {message}")]
    PluginError { plugin: String, message: String },

    #[error("Plugin '{plugin}' returned invalid output: {reason}")]
    InvalidOutput { plugin: String, reason: String },

    #[error("Failed to compile WASM module '{path}': {source}")]
    CompilationFailed {
        path: PathBuf,
        #[source]
        source: wasmtime::Error,
    },

    #[error("Host function '{function}' failed: {message}")]
    HostFunctionError { function: String, message: String },

    #[error("Plugin file not found: {path}")]
    PluginNotFound { path: PathBuf },

    #[error("Failed to instantiate plugin '{plugin}': {source}")]
    InstantiationFailed {
        plugin: String,
        #[source]
        source: wasmtime::Error,
    },

    #[error("Plugin '{plugin}' missing required export: {export}")]
    MissingExport { plugin: String, export: String },

    #[error("Plugin '{plugin}' trapped: {message}")]
    Trap { plugin: String, message: String },

    #[error("Plugin '{plugin}' failed to initialize: {message}")]
    InitializationFailed { plugin: String, message: String },

    #[error("Plugin '{plugin}' exceeded memory limit ({limit_bytes} bytes)")]
    MemoryExceeded { plugin: String, limit_bytes: usize },

    #[error("Plugin '{plugin}' exceeded execution fuel (limit: {fuel_limit})")]
    FuelExhausted { plugin: String, fuel_limit: u64 },

    #[error("Plugin '{plugin}' timed out after {timeout_ms}ms")]
    Timeout { plugin: String, timeout_ms: u64 },

    #[error("Plugin not loaded: {name}")]
    PluginNotLoaded { name: String },
}
