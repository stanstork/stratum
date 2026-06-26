use crate::{
    error::WasmError,
    runtime::limits::{HostCapabilities, ResourceLimits},
};
use tracing::{debug, error, info, warn};
use wasmtime::{Caller, Linker, ResourceLimiter, StoreLimits};

pub struct PluginState {
    pub plugin_name: String,
    pub capabilities: HostCapabilities,
    pub limits: StoreLimits,
    pub wasi_ctx: wasmtime_wasi::preview1::WasiP1Ctx,
}

impl PluginState {
    pub fn new(
        plugin_name: String,
        capabilities: HostCapabilities,
        limits: &ResourceLimits,
    ) -> Self {
        let wasi_ctx = wasmtime_wasi::WasiCtxBuilder::new()
            .inherit_stdio() // Route to host stdout/stderr (captured by tracing)
            .build_p1();
        Self {
            plugin_name,
            capabilities,
            limits: limits.to_store_limits(),
            wasi_ctx,
        }
    }
}

impl ResourceLimiter for PluginState {
    fn memory_growing(
        &mut self,
        current: usize,
        desired: usize,
        maximum: Option<usize>,
    ) -> anyhow::Result<bool> {
        self.limits.memory_growing(current, desired, maximum)
    }

    fn table_growing(
        &mut self,
        current: usize,
        desired: usize,
        maximum: Option<usize>,
    ) -> anyhow::Result<bool> {
        self.limits.table_growing(current, desired, maximum)
    }
}

/// Link all host functions into the Wasmtime Linker.
pub fn link_host_functions(linker: &mut Linker<PluginState>) -> Result<(), WasmError> {
    link_logging(linker)?;
    link_http_stub(linker)?;
    link_kv_stub(linker)?;
    link_metrics_stub(linker)?;
    Ok(())
}

/// Logging host functions. Read a UTF-8 string from guest memory and emit via tracing.
fn link_logging(linker: &mut Linker<PluginState>) -> Result<(), WasmError> {
    // Helper: read string from guest memory
    fn read_guest_string(
        caller: &mut Caller<'_, PluginState>,
        ptr: u32,
        len: u32,
    ) -> Option<String> {
        let memory = caller.get_export("memory")?.into_memory()?;
        let data = memory.data(caller);
        let start = ptr as usize;
        // Use checked_add so a malicious guest can't overflow the index
        // computation (e.g. on 32-bit hosts) and trigger an out-of-bounds panic.
        let end = start.checked_add(len as usize)?;
        if end > data.len() {
            return None;
        }
        String::from_utf8(data[start..end].to_vec()).ok()
    }

    linker
        .func_wrap(
            "stratum",
            "log_debug",
            |mut caller: Caller<'_, PluginState>, ptr: u32, len: u32| {
                if caller.data().capabilities.logging
                    && let Some(msg) = read_guest_string(&mut caller, ptr, len)
                {
                    debug!(plugin = %caller.data().plugin_name, "{}", msg);
                }
            },
        )
        .map_err(|e| link_err("log_debug", e))?;

    linker
        .func_wrap(
            "stratum",
            "log_info",
            |mut caller: Caller<'_, PluginState>, ptr: u32, len: u32| {
                if caller.data().capabilities.logging
                    && let Some(msg) = read_guest_string(&mut caller, ptr, len)
                {
                    info!(plugin = %caller.data().plugin_name, "{}", msg);
                }
            },
        )
        .map_err(|e| link_err("log_info", e))?;

    linker
        .func_wrap(
            "stratum",
            "log_warn",
            |mut caller: Caller<'_, PluginState>, ptr: u32, len: u32| {
                if caller.data().capabilities.logging
                    && let Some(msg) = read_guest_string(&mut caller, ptr, len)
                {
                    warn!(plugin = %caller.data().plugin_name, "{}", msg);
                }
            },
        )
        .map_err(|e| link_err("log_warn", e))?;

    linker
        .func_wrap(
            "stratum",
            "log_error",
            |mut caller: Caller<'_, PluginState>, ptr: u32, len: u32| {
                if caller.data().capabilities.logging
                    && let Some(msg) = read_guest_string(&mut caller, ptr, len)
                {
                    error!(plugin = %caller.data().plugin_name, "{}", msg);
                }
            },
        )
        .map_err(|e| link_err("log_error", e))?;

    Ok(())
}

/// HTTP stub - returns 0 (capability denied / not implemented). Shared by the
/// Rust SDK and the JS runtime: `(method, url_ptr, url_len, body_ptr, body_len)
/// -> u64` packed (ptr, len) of the response body, or 0 on error.
fn link_http_stub(linker: &mut Linker<PluginState>) -> Result<(), WasmError> {
    linker
        .func_wrap(
            "stratum",
            "http_request",
            |_caller: Caller<'_, PluginState>,
             _method: u32,
             _url_ptr: u32,
             _url_len: u32,
             _body_ptr: u32,
             _body_len: u32|
             -> u64 {
                // TODO: Not implemented yet; 0 signals "no response" to both guests.
                0
            },
        )
        .map_err(|e| link_err("http_request", e))?;
    Ok(())
}

/// KV stub - returns error if not enabled.
fn link_kv_stub(linker: &mut Linker<PluginState>) -> Result<(), WasmError> {
    linker
        .func_wrap(
            "stratum",
            "kv_get",
            |_caller: Caller<'_, PluginState>, _key_ptr: u32, _key_len: u32| -> (u32, u32) {
                (0, 0) // TODO: Not implemented
            },
        )
        .map_err(|e| link_err("kv_get", e))?;

    linker
        .func_wrap(
            "stratum",
            "kv_set",
            |_caller: Caller<'_, PluginState>,
             _key_ptr: u32,
             _key_len: u32,
             _val_ptr: u32,
             _val_len: u32| {
                // TODO: Not implemented
            },
        )
        .map_err(|e| link_err("kv_set", e))?;

    Ok(())
}

/// Metrics stub.
fn link_metrics_stub(linker: &mut Linker<PluginState>) -> Result<(), WasmError> {
    linker
        .func_wrap(
            "stratum",
            "metric_counter",
            |_caller: Caller<'_, PluginState>, _name_ptr: u32, _name_len: u32, _value: i64| {
                // TODO: Not implemented
            },
        )
        .map_err(|e| link_err("metric_counter", e))?;

    linker
        .func_wrap(
            "stratum",
            "metric_gauge",
            |_caller: Caller<'_, PluginState>, _name_ptr: u32, _name_len: u32, _value: f64| {
                // TODO: Not implemented
            },
        )
        .map_err(|e| link_err("metric_gauge", e))?;

    Ok(())
}

fn link_err(name: &str, e: anyhow::Error) -> WasmError {
    WasmError::HostFunctionError {
        function: name.to_string(),
        message: e.to_string(),
    }
}
