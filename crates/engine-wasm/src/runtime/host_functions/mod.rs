use crate::{
    error::WasmError,
    runtime::limits::{HostCapabilities, ResourceLimits},
};
use std::collections::HashMap;
use wasmtime::{Caller, Linker, ResourceLimiter, StoreLimits};

mod http;
mod kv;
mod logging;
mod metrics;

pub struct PluginState {
    pub plugin_name: String,
    pub capabilities: HostCapabilities,
    pub limits: StoreLimits,
    pub wasi_ctx: wasmtime_wasi::preview1::WasiP1Ctx,
    /// Instance-scoped scratch key-value store, gated by the `key_value_store` capability.
    pub kv: HashMap<Vec<u8>, Vec<u8>>,
}

impl PluginState {
    pub fn new(
        plugin_name: String,
        capabilities: HostCapabilities,
        limits: &ResourceLimits,
    ) -> Result<Self, WasmError> {
        use wasmtime_wasi::{DirPerms, FilePerms};

        let mut builder = wasmtime_wasi::WasiCtxBuilder::new();
        builder.inherit_stdio(); // Route to host stdout/stderr (captured by tracing)

        // Expose only the explicitly-granted environment variables.
        for (name, value) in &capabilities.env {
            builder.env(name, value);
        }

        // Preopen only the explicitly-granted directories. A missing directory
        // is a hard error rather than a silent no-grant.
        for dir in &capabilities.fs_read {
            builder
                .preopened_dir(dir, dir.to_string_lossy(), DirPerms::READ, FilePerms::READ)
                .map_err(|e| WasmError::WasiSetup {
                    plugin: plugin_name.clone(),
                    message: format!("preopen read dir {}: {e}", dir.display()),
                })?;
        }

        for dir in &capabilities.fs_write {
            builder
                .preopened_dir(
                    dir,
                    dir.to_string_lossy(),
                    DirPerms::all(),
                    FilePerms::all(),
                )
                .map_err(|e| WasmError::WasiSetup {
                    plugin: plugin_name.clone(),
                    message: format!("preopen write dir {}: {e}", dir.display()),
                })?;
        }

        Ok(Self {
            plugin_name,
            capabilities,
            limits: limits.to_store_limits(),
            wasi_ctx: builder.build_p1(),
            kv: HashMap::new(),
        })
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

/// Link every capability's host functions into the Wasmtime `Linker`.
pub fn link_host_functions(linker: &mut Linker<PluginState>) -> Result<(), WasmError> {
    logging::link(linker)?;
    http::link(linker)?;
    kv::link(linker)?;
    metrics::link(linker)?;
    Ok(())
}

/// Read `len` bytes at `ptr` from the guest's linear memory. Returns `None` if
/// the guest has no `memory` export or the range is out of bounds.
fn read_guest_bytes(caller: &mut Caller<'_, PluginState>, ptr: u32, len: u32) -> Option<Vec<u8>> {
    let memory = caller.get_export("memory")?.into_memory()?;

    let start = ptr as usize;
    let end = start.checked_add(len as usize)?;

    memory
        .data(caller)
        .get(start..end)
        .map(|slice| slice.to_vec())
}

/// Read a UTF-8 string at `ptr`/`len` from guest memory, or `None` if the range
/// is invalid or not valid UTF-8.
fn read_guest_string(caller: &mut Caller<'_, PluginState>, ptr: u32, len: u32) -> Option<String> {
    let bytes = read_guest_bytes(caller, ptr, len)?;
    String::from_utf8(bytes).ok()
}

/// Copy `data` into freshly-allocated guest memory and return the packed
/// `(ptr, len)` the guest ABI expects (`ptr` in the high 32 bits).
fn write_guest_bytes(caller: &mut Caller<'_, PluginState>, data: &[u8]) -> Option<u64> {
    if data.is_empty() {
        return None;
    }

    let len = u32::try_from(data.len()).ok()?;

    // Call the guest's own allocator so the buffer lives in its heap.
    let alloc = caller.get_export("__stratum_alloc")?.into_func()?;
    let alloc = alloc.typed::<u32, u32>(&caller).ok()?;

    let ptr = alloc.call(&mut *caller, len).ok()?;
    if ptr == 0 {
        return None;
    }

    let memory = caller.get_export("memory")?.into_memory()?;
    memory.write(&mut *caller, ptr as usize, data).ok()?;

    Some(((ptr as u64) << 32) | (len as u64))
}

fn link_err(name: &str, e: anyhow::Error) -> WasmError {
    WasmError::HostFunctionError {
        function: name.to_string(),
        message: e.to_string(),
    }
}

/// Shared test harness used by the per-capability test modules: instantiate a
/// hand-written WAT guest with the real host functions linked, under a given set
/// of capabilities.
#[cfg(test)]
pub(crate) mod test_harness {
    use super::{PluginState, link_host_functions};
    use crate::runtime::limits::{HostCapabilities, ResourceLimits};
    use wasmtime::{Engine, Instance, Linker, Module, Store};

    /// Instantiate `wat` with the real host functions linked under `caps`, and
    /// return `(store, instance)` ready to call.
    pub(crate) fn instantiate(wat: &str, caps: HostCapabilities) -> (Store<PluginState>, Instance) {
        let engine = Engine::default();
        let mut linker = Linker::new(&engine);
        link_host_functions(&mut linker).expect("link host functions");

        let limits = ResourceLimits::for_io_plugins();
        let state = PluginState::new("test-plugin".into(), caps, &limits).expect("wasi setup");
        let mut store = Store::new(&engine, state);

        let module = Module::new(&engine, wat).expect("compile wat");
        let instance = linker
            .instantiate(&mut store, &module)
            .expect("instantiate");
        (store, instance)
    }

    /// Capabilities with only `key_value_store` / `metrics` toggled.
    pub(crate) fn caps(kv: bool, metrics: bool) -> HostCapabilities {
        HostCapabilities {
            key_value_store: kv,
            metrics,
            ..HostCapabilities::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::PluginState;
    use crate::error::WasmError;
    use crate::runtime::limits::{HostCapabilities, ResourceLimits};

    #[test]
    fn preopen_grants_an_existing_dir_and_rejects_a_missing_one() {
        let limits = ResourceLimits::for_io_plugins();

        // An existing directory preopens successfully.
        let ok_caps = HostCapabilities {
            fs_read: vec![std::env::temp_dir()],
            ..HostCapabilities::default()
        };
        assert!(PluginState::new("p".into(), ok_caps, &limits).is_ok());

        // A missing directory is a hard WasiSetup error, not a silent no-grant.
        let missing = std::env::temp_dir().join("stratum-does-not-exist-a1b2c3");
        let bad_caps = HostCapabilities {
            fs_read: vec![missing],
            ..HostCapabilities::default()
        };
        assert!(matches!(
            PluginState::new("p".into(), bad_caps, &limits),
            Err(WasmError::WasiSetup { .. })
        ));
    }

    #[test]
    fn env_grant_is_applied_without_error() {
        // Smoke test: the resolved env pairs are handed to the WASI builder. (A
        // full guest-reads-env assertion would need a WASI-aware guest module.)
        let limits = ResourceLimits::for_io_plugins();
        let caps = HostCapabilities {
            env: vec![("API_KEY".into(), "s3cr3t".into())],
            ..HostCapabilities::default()
        };
        assert!(PluginState::new("p".into(), caps, &limits).is_ok());
    }
}
