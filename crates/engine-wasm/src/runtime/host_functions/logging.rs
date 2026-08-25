use super::{PluginState, link_err, read_guest_string};
use crate::error::WasmError;
use tracing::{debug, error, info, warn};
use wasmtime::{Caller, Linker};

pub(super) fn link(linker: &mut Linker<PluginState>) -> Result<(), WasmError> {
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
