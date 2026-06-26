pub mod error;
pub mod exchange;
pub mod filter;
pub mod host;
pub mod input;
pub mod output;
pub mod record;
pub mod runtime;
pub mod schema;
pub mod sink;
pub mod source;
pub mod value;

// Re-export the proc macros at crate root.
pub use stratum_plugin_sdk_macros::{
    stratum_filter, stratum_sink, stratum_source, stratum_transform,
};

// Re-export primary types for `use stratum_plugin_sdk::*;`.
pub use error::{PluginError, PluginErrorCode, PluginResult};
pub use filter::FilterDecision;
pub use input::PluginInput;
pub use output::PluginOutput;
pub use record::{FieldValue, Record};
pub use schema::{PluginField, PluginMetadata, PluginType};
pub use sink::{PluginBatch, SinkConfig, WriteResult};
pub use source::{SourceConfig, SourcePage};
pub use value::Value;

// Host wrappers.
pub use host::http::{http_get, http_post, HttpResponse};
pub use host::kv::{kv_get, kv_set};
pub use host::log::{log_debug, log_error, log_info, log_warn};
pub use host::metrics::{metric_counter, metric_gauge};

// Runtime helpers - used by macro-generated code.
pub use runtime::pack::{pack, unpack};

use std::collections::HashMap;
use std::sync::OnceLock;

/// Static configuration delivered to a plugin at initialize time, parsed from
/// the SMQL `plugin "x" { config { ... } }` block.
#[derive(Debug, Clone, Default)]
pub struct PluginConfig {
    params: HashMap<String, String>,
}

impl PluginConfig {
    pub fn new(params: HashMap<String, String>) -> Self {
        Self { params }
    }

    /// Look up a config value; `None` if the key was not supplied.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.params.get(key).map(String::as_str)
    }

    /// Look up a config value, erroring if absent.
    pub fn require(&self, key: &str) -> PluginResult<&str> {
        self.get(key)
            .ok_or_else(|| PluginError::invalid_input(format!("missing config key: '{}'", key)))
    }
}

static PLUGIN_CONFIG: OnceLock<PluginConfig> = OnceLock::new();
static SOURCE_CONFIG: OnceLock<SourceConfig> = OnceLock::new();
static SINK_CONFIG: OnceLock<SinkConfig> = OnceLock::new();

/// Access the plugin's static configuration.
pub fn config() -> &'static PluginConfig {
    PLUGIN_CONFIG.get_or_init(PluginConfig::default)
}

#[doc(hidden)]
pub fn __set_plugin_config(cfg: PluginConfig) {
    let _ = PLUGIN_CONFIG.set(cfg);
}

/// Access the source plugin's static configuration. Returns an error if the
/// plugin's `__stratum_initialize` hasn't run yet.
pub fn source_config() -> PluginResult<&'static SourceConfig> {
    SOURCE_CONFIG
        .get()
        .ok_or_else(|| PluginError::internal("source plugin not initialized"))
}

/// Access the sink plugin's static configuration. Returns an error if the
/// plugin's `__stratum_initialize` hasn't run yet.
pub fn sink_config() -> PluginResult<&'static SinkConfig> {
    SINK_CONFIG
        .get()
        .ok_or_else(|| PluginError::internal("sink plugin not initialized"))
}

#[doc(hidden)]
pub fn __set_source_config(cfg: SourceConfig) {
    let _ = SOURCE_CONFIG.set(cfg);
}

#[doc(hidden)]
pub fn __set_sink_config(cfg: SinkConfig) {
    let _ = SINK_CONFIG.set(cfg);
}
