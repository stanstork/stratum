use crate::io::sink::Sink;
use async_trait::async_trait;
use connectors::{error::DriverError, sql::metadata::table::TableMetadata};
use engine_wasm::runtime::instance::PluginInstance;
use model::records::Record;
use std::sync::{Arc, Mutex};

pub struct WasmSinkAdapter {
    plugin: Arc<Mutex<PluginInstance>>,
    plugin_name: String,
}

impl WasmSinkAdapter {
    pub fn new(plugin: PluginInstance) -> Self {
        let plugin_name = plugin.plugin_name().to_string();
        Self {
            plugin: Arc::new(Mutex::new(plugin)),
            plugin_name,
        }
    }
}

#[async_trait]
impl Sink for WasmSinkAdapter {
    /// Run the plugin's `__stratum_prepare` once before the first batch. No-op
    /// if the plugin defines no prepare hook.
    async fn prepare(&self) -> Result<(), DriverError> {
        let plugin = self.plugin.clone();
        let plugin_name = self.plugin_name.clone();

        tokio::task::spawn_blocking(move || {
            let mut guard = plugin.lock().expect("wasm sink plugin mutex poisoned");
            guard.call_prepare()
        })
        .await
        .map_err(|e| {
            DriverError::Unknown(format!(
                "wasm sink '{plugin_name}' prepare join failed: {e}"
            ))
        })?
        .map_err(|e| {
            DriverError::QueryError(format!("wasm sink '{plugin_name}' prepare failed: {e}"))
        })
    }

    /// Hand the batch to the plugin's `__stratum_write_batch`. The destination
    /// table is implied by the plugin's config (set at init), so `meta` is not
    /// forwarded - the wire payload carries only the records.
    async fn write_batch(
        &self,
        _meta: &TableMetadata,
        rows: &[Record],
    ) -> Result<u64, DriverError> {
        // WASM calls are synchronous and can run for a while; keep them off the
        // async runtime worker by hopping onto a blocking thread.
        let plugin = self.plugin.clone();
        let plugin_name = self.plugin_name.clone();
        let rows = rows.to_vec();

        let written = tokio::task::spawn_blocking(move || {
            let mut guard = plugin.lock().expect("wasm sink plugin mutex poisoned");
            guard.call_write_batch(&rows)
        })
        .await
        .map_err(|e| {
            DriverError::Unknown(format!("wasm sink '{plugin_name}' task join failed: {e}"))
        })?
        .map_err(|e| {
            DriverError::QueryError(format!("wasm sink '{plugin_name}' write_batch failed: {e}"))
        })?;

        Ok(written.rows_written)
    }

    async fn finalize(&self) -> Result<(), DriverError> {
        let plugin = self.plugin.clone();
        let plugin_name = self.plugin_name.clone();

        tokio::task::spawn_blocking(move || {
            let mut guard = plugin.lock().expect("wasm sink plugin mutex poisoned");
            guard.call_finalize()
        })
        .await
        .map_err(|e| {
            DriverError::Unknown(format!(
                "wasm sink '{plugin_name}' finalize join failed: {e}"
            ))
        })?
        .map_err(|e| DriverError::QueryError(format!("wasm sink finalize failed: {e}")))
    }
}
