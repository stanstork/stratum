//! Write [Paganel](https://github.com/stanstork/paganel) plugins in Rust.
//!
//! Paganel is a data migration engine. A plugin runs inside its WASM sandbox as
//! a **transform** (compute a column), a **filter** (accept or reject a row), a
//! **source** (produce rows) or a **sink** (consume them). Pick the matching
//! attribute macro and write an ordinary function: the macro emits the ABI the
//! host expects.
//!
//! ```no_run
//! use paganel_plugin_sdk::{paganel_transform, PluginInput, PluginResult};
//!
//! #[paganel_transform(
//!     name = "adder",
//!     version = "1.0.0",
//!     output = "f64",
//!     input = [
//!         { name = "a", type = "f64", nullable = false },
//!         { name = "b", type = "f64", nullable = false },
//!     ]
//! )]
//! fn add(inputs: Vec<PluginInput>) -> PluginResult<Vec<f64>> {
//!     inputs
//!         .iter()
//!         .map(|input| Ok(input.get_f64("a")? + input.get_f64("b")?))
//!         .collect()
//! }
//! ```
//!
//! Build with `--target wasm32-wasip1 --release` and a `cdylib` crate type.
//!
//! Your function receives a whole batch, not one row: the host crosses the WASM
//! boundary once per batch, which is why a native Rust plugin runs close to the
//! no-plugin rate. Plugins execute under fuel, memory and timeout caps with no
//! filesystem or network access unless the pipeline grants it.
//!
//! # Where to look
//!
//! - [`input`] / [`output`] - the values your function reads and returns
//! - [`filter`], [`source`], [`sink`] - the other roles
//! - [`host`] - host functions a pipeline can grant (logging, HTTP, key-value)
//! - [`error`] - failing a row versus failing the run

pub mod columnar;
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
pub use paganel_plugin_sdk_macros::{
    paganel_filter, paganel_sink, paganel_source, paganel_transform,
};

// Re-export primary types for `use paganel_plugin_sdk::*;`.
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
pub use host::env::env_get;
pub use host::fs::{fs_read, fs_read_to_string, fs_write};
pub use host::http::{HttpResponse, http_get, http_post};
pub use host::kv::{kv_get, kv_set};
pub use host::log::{log_debug, log_error, log_info, log_warn};
pub use host::metrics::{metric_counter, metric_gauge};

// Runtime helpers - used by macro-generated code.
pub use runtime::pack::{pack, unpack};

use std::collections::HashMap;
use std::sync::OnceLock;

/// Static configuration delivered to a plugin at initialize time, parsed from
/// the PPL `plugin "x" { config { ... } }` block.
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
/// plugin's `__paganel_initialize` hasn't run yet.
pub fn source_config() -> PluginResult<&'static SourceConfig> {
    SOURCE_CONFIG
        .get()
        .ok_or_else(|| PluginError::internal("source plugin not initialized"))
}

/// Access the sink plugin's static configuration. Returns an error if the
/// plugin's `__paganel_initialize` hasn't run yet.
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
