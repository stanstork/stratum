use crate::{
    error::WasmError,
    exchange::{
        json_v1,
        types::{FilterDecision, PluginInput, PluginOutput, WriteResult},
    },
    runtime::{
        host_functions::PluginState,
        limits::{HostCapabilities, ResourceLimits},
    },
    schema::{PluginMetadata, PluginType},
};
use model::records::Record;
use tracing::info;
use wasmtime::{Engine, Instance, Linker, Module, Store, TypedFunc};

/// A live plugin instance. One per pipeline per plugin.
pub struct PluginInstance {
    store: Store<PluginState>,
    instance: wasmtime::Instance,

    // Required exports (all plugins)
    alloc_fn: TypedFunc<u32, u32>,
    dealloc_fn: TypedFunc<(u32, u32), ()>,

    // Role-specific exports (populated based on plugin type)
    transform_fn: Option<TypedFunc<(u32, u32), u64>>,
    evaluate_fn: Option<TypedFunc<(u32, u32), u64>>,
    read_page_fn: Option<TypedFunc<(u32, u32), u64>>,
    write_batch_fn: Option<TypedFunc<(u32, u32), u64>>,
    prepare_fn: Option<TypedFunc<(u32, u32), u32>>,
    finalize_fn: Option<TypedFunc<(), u32>>,

    // Cached metadata (loaded once at init)
    metadata: PluginMetadata,
    limits: ResourceLimits,
}

impl PluginInstance {
    pub(crate) fn new(
        engine: &Engine,
        linker: &Linker<PluginState>,
        module: &Module,
        plugin_name: String,
        capabilities: HostCapabilities,
        limits: ResourceLimits,
        config_json: Option<&[u8]>,
    ) -> Result<Self, WasmError> {
        // Create store with resource limiter
        let state = PluginState::new(plugin_name.clone(), capabilities, &limits);
        let mut store = Store::new(engine, state);
        store.limiter(|state| state);
        store
            .set_fuel(limits.max_execution_fuel)
            .map_err(|e| WasmError::InstantiationFailed {
                plugin: plugin_name.clone(),
                source: e,
            })?;

        // Set epoch deadline (timeout_ms / 100ms per tick)
        let epoch_ticks = (limits.timeout_ms / 100).max(1);
        store.epoch_deadline_trap();
        store.set_epoch_deadline(epoch_ticks);

        let instance =
            linker
                .instantiate(&mut store, module)
                .map_err(|e| WasmError::InstantiationFailed {
                    plugin: plugin_name.clone(),
                    source: e,
                })?;

        // Resolve required exports
        let alloc_fn = Self::get_typed_func::<u32, u32>(
            &mut store,
            &instance,
            &plugin_name,
            "__stratum_alloc",
        )?;
        let dealloc_fn = Self::get_typed_func::<(u32, u32), ()>(
            &mut store,
            &instance,
            &plugin_name,
            "__stratum_dealloc",
        )?;

        // Resolve role-specific exports (optional - determined by which ones exist)
        let transform_fn = Self::try_get_typed_func(&mut store, &instance, "__stratum_transform");
        let evaluate_fn = Self::try_get_typed_func(&mut store, &instance, "__stratum_evaluate");
        let read_page_fn = Self::try_get_typed_func(&mut store, &instance, "__stratum_read_page");
        let write_batch_fn =
            Self::try_get_typed_func(&mut store, &instance, "__stratum_write_batch");
        let prepare_fn = Self::try_get_typed_func(&mut store, &instance, "__stratum_prepare");
        let finalize_fn = Self::try_get_typed_func(&mut store, &instance, "__stratum_finalize");

        // Load metadata
        let metadata_fn = Self::get_typed_func::<(), u64>(
            &mut store,
            &instance,
            &plugin_name,
            "__stratum_metadata",
        )?;
        let metadata = Self::load_metadata(
            &mut store,
            &instance,
            &metadata_fn,
            &dealloc_fn,
            &plugin_name,
        )?;

        // Initialize plugin with config
        let init_fn = Self::get_typed_func::<(u32, u32), u32>(
            &mut store,
            &instance,
            &plugin_name,
            "__stratum_initialize",
        )?;

        let config_bytes = config_json.unwrap_or(b"{}");
        let status = Self::call_with_bytes(
            &mut store,
            &instance,
            &alloc_fn,
            &dealloc_fn,
            &init_fn,
            config_bytes,
            &plugin_name,
        )?;

        if status != 0 {
            return Err(WasmError::InitializationFailed {
                plugin: plugin_name,
                message: format!("initialize() returned status code {}", status),
            });
        }

        info!(plugin = %plugin_name, plugin_type = ?metadata.plugin_type, version = %metadata.version, "Plugin initialized");

        Ok(Self {
            store,
            instance,
            alloc_fn,
            dealloc_fn,
            transform_fn,
            evaluate_fn,
            read_page_fn,
            write_batch_fn,
            prepare_fn,
            finalize_fn,
            metadata,
            limits,
        })
    }

    pub fn metadata(&self) -> &PluginMetadata {
        &self.metadata
    }

    pub fn plugin_name(&self) -> &str {
        &self.metadata.name
    }

    pub fn plugin_type(&self) -> PluginType {
        self.metadata.plugin_type
    }

    /// Call a transform plugin. Returns the computed value.
    pub fn call_transform(&mut self, input: &PluginInput) -> Result<PluginOutput, WasmError> {
        let func = self
            .transform_fn
            .as_ref()
            .ok_or_else(|| WasmError::MissingExport {
                plugin: self.metadata.name.clone(),
                export: "__stratum_transform".to_string(),
            })?
            .clone();
        let input_bytes = json_v1::serialize_input(input, &self.metadata.input_schema)?;
        let output_bytes = self.call_data_fn(func, &input_bytes)?;

        json_v1::deserialize_output(&output_bytes, &self.metadata.name)
    }

    /// Call a filter plugin. Returns pass/reject.
    pub fn call_evaluate(&mut self, input: &PluginInput) -> Result<FilterDecision, WasmError> {
        let func = self
            .evaluate_fn
            .as_ref()
            .ok_or_else(|| WasmError::MissingExport {
                plugin: self.metadata.name.clone(),
                export: "__stratum_evaluate".to_string(),
            })?
            .clone();
        let input_bytes = json_v1::serialize_input(input, &self.metadata.input_schema)?;
        let output_bytes = self.call_data_fn(func, &input_bytes)?;
        json_v1::deserialize_filter_decision(&output_bytes, &self.metadata.name)
    }

    /// Call a source plugin's read_page. The host treats the cursor as opaque
    /// and round-trips it verbatim to the plugin.
    pub fn call_read_page(
        &mut self,
        cursor: Option<&str>,
        _batch_size: usize,
    ) -> Result<crate::exchange::types::SourcePage, WasmError> {
        let func = self
            .read_page_fn
            .as_ref()
            .ok_or_else(|| WasmError::MissingExport {
                plugin: self.metadata.name.clone(),
                export: "__stratum_read_page".to_string(),
            })?
            .clone();
        let input_bytes = json_v1::serialize_cursor(cursor)?;
        let output_bytes = self.call_data_fn(func, &input_bytes)?;
        json_v1::deserialize_source_page(&output_bytes, &self.metadata.name)
    }

    /// Call a sink plugin's write_batch. Returns how many rows the plugin
    /// reports as committed. The destination (table / endpoint / file) is
    /// supplied to the plugin via its config at init time, so the wire
    /// payload carries only the records.
    pub fn call_write_batch(&mut self, rows: &[Record]) -> Result<WriteResult, WasmError> {
        let func = self
            .write_batch_fn
            .as_ref()
            .ok_or_else(|| WasmError::MissingExport {
                plugin: self.metadata.name.clone(),
                export: "__stratum_write_batch".to_string(),
            })?
            .clone();
        let batch = crate::exchange::types::PluginBatch {
            records: rows.to_vec(),
        };
        let input_bytes = json_v1::serialize_batch(&batch)?;
        let output_bytes = self.call_data_fn(func, &input_bytes)?;
        json_v1::deserialize_write_result(&output_bytes, &self.metadata.name)
    }

    /// Call a sink plugin's prepare hook (`__stratum_prepare`). Invoked once
    /// before the first batch so the plugin can open connections, create staging tables, etc.
    pub fn call_prepare(&mut self) -> Result<(), WasmError> {
        let Some(func) = self.prepare_fn.as_ref().cloned() else {
            return Ok(());
        };
        self.reset_fuel_and_epoch();
        let status = func
            .call(&mut self.store, (0, 0))
            .map_err(|e| self.classify_trap(e, "prepare"))?;
        if status != 0 {
            return Err(WasmError::PluginError {
                plugin: self.metadata.name.clone(),
                message: format!("prepare() returned status code {}", status),
            });
        }
        Ok(())
    }

    /// Call a sink plugin's finalize hook (`__stratum_finalize`). Invoked once
    /// after the final batch so the plugin can flush buffers or commit.
    pub fn call_finalize(&mut self) -> Result<(), WasmError> {
        let Some(func) = self.finalize_fn.as_ref().cloned() else {
            return Ok(());
        };
        self.reset_fuel_and_epoch();
        let status = func
            .call(&mut self.store, ())
            .map_err(|e| self.classify_trap(e, "finalize"))?;
        if status != 0 {
            return Err(WasmError::PluginError {
                plugin: self.metadata.name.clone(),
                message: format!("finalize() returned status code {}", status),
            });
        }
        Ok(())
    }

    pub fn read_metadata(
        engine: &Engine,
        linker: &Linker<PluginState>,
        module: &Module,
    ) -> Result<PluginMetadata, WasmError> {
        let state = PluginState::new(
            "<inspect>".into(),
            HostCapabilities::default(),
            &ResourceLimits::for_io_plugins(),
        );
        let mut store = Store::new(engine, state);
        store.limiter(|s| s);
        let _ = store.set_fuel(ResourceLimits::for_io_plugins().max_execution_fuel);
        // The engine enables epoch interruption, so a deadline must be set or
        // the first epoch check traps (default deadline is 0).
        store.epoch_deadline_trap();
        store.set_epoch_deadline((ResourceLimits::for_io_plugins().timeout_ms / 100).max(1));
        let instance =
            linker
                .instantiate(&mut store, module)
                .map_err(|e| WasmError::InstantiationFailed {
                    plugin: "<inspect>".into(),
                    source: e,
                })?;
        let dealloc = Self::get_typed_func::<(u32, u32), ()>(
            &mut store,
            &instance,
            "<inspect>",
            "__stratum_dealloc",
        )?;
        let meta_fn = Self::get_typed_func::<(), u64>(
            &mut store,
            &instance,
            "<inspect>",
            "__stratum_metadata",
        )?;
        Self::load_metadata(&mut store, &instance, &meta_fn, &dealloc, "<inspect>")
    }

    fn load_metadata(
        store: &mut Store<PluginState>,
        instance: &Instance,
        metadata_fn: &TypedFunc<(), u64>,
        dealloc_fn: &TypedFunc<(u32, u32), ()>,
        plugin: &str,
    ) -> Result<PluginMetadata, WasmError> {
        let packed = metadata_fn
            .call(&mut *store, ())
            .map_err(|e| WasmError::Trap {
                plugin: plugin.to_string(),
                message: format!("metadata() failed: {}", e),
            })?;

        let ptr = (packed >> 32) as u32;
        let len = (packed & 0xFFFFFFFF) as u32;

        let memory =
            instance
                .get_memory(&mut *store, "memory")
                .ok_or_else(|| WasmError::MissingExport {
                    plugin: plugin.to_string(),
                    export: "memory".to_string(),
                })?;

        let mut bytes = vec![0u8; len as usize];
        memory
            .read(&mut *store, ptr as usize, &mut bytes)
            .map_err(|e| WasmError::Trap {
                plugin: plugin.to_string(),
                message: format!("failed to read metadata: {}", e),
            })?;

        // Deallocate guest memory after reading
        let _ = dealloc_fn.call(store, (ptr, len));

        PluginMetadata::from_json(&bytes, plugin)
    }

    /// Call a function that takes bytes and returns a status code (u32).
    fn call_with_bytes(
        store: &mut Store<PluginState>,
        instance: &Instance,
        alloc_fn: &TypedFunc<u32, u32>,
        dealloc_fn: &TypedFunc<(u32, u32), ()>,
        func: &TypedFunc<(u32, u32), u32>,
        bytes: &[u8],
        plugin: &str,
    ) -> Result<u32, WasmError> {
        let len = bytes.len() as u32;

        // Allocate and write
        let ptr = alloc_fn
            .call(&mut *store, len)
            .map_err(|e| WasmError::Trap {
                plugin: plugin.to_string(),
                message: format!("alloc failed: {}", e),
            })?;

        let memory =
            instance
                .get_memory(&mut *store, "memory")
                .ok_or_else(|| WasmError::MissingExport {
                    plugin: plugin.to_string(),
                    export: "memory".to_string(),
                })?;

        memory
            .write(&mut *store, ptr as usize, bytes)
            .map_err(|e| WasmError::Trap {
                plugin: plugin.to_string(),
                message: format!("failed to write input bytes: {}", e),
            })?;

        // Capture the call result first so the input allocation is always freed,
        // even when the plugin traps or returns an error.
        let call_result = func.call(&mut *store, (ptr, len));

        // Deallocate (unconditionally)
        let _ = dealloc_fn.call(store, (ptr, len));

        let status = call_result.map_err(|e| WasmError::Trap {
            plugin: plugin.to_string(),
            message: format!("call failed: {}", e),
        })?;

        Ok(status)
    }

    /// Generic data call: write input bytes to guest, call function, read output bytes.
    fn call_data_fn(
        &mut self,
        func: TypedFunc<(u32, u32), u64>,
        input_bytes: &[u8],
    ) -> Result<Vec<u8>, WasmError> {
        self.reset_fuel_and_epoch();

        let input_len = input_bytes.len() as u32;

        // Allocate guest memory for input
        let input_ptr = self
            .alloc_fn
            .call(&mut self.store, input_len)
            .map_err(|e| self.classify_trap(e, "alloc (input)"))?;

        // Write input to guest memory
        let mem = self.get_memory()?;
        mem.write(&mut self.store, input_ptr as usize, input_bytes)
            .map_err(|e| WasmError::Trap {
                plugin: self.metadata.name.clone(),
                message: format!("failed to write input to guest memory: {}", e),
            })?;

        // Capture the call result first so the input allocation is always freed,
        // even when the plugin traps or returns an error.
        let call_result = func.call(&mut self.store, (input_ptr, input_len));

        // Dealloc input (unconditionally)
        let _ = self
            .dealloc_fn
            .call(&mut self.store, (input_ptr, input_len));

        let packed_result =
            call_result.map_err(|e| self.classify_trap(e, "plugin function call"))?;

        // Unpack result: high 32 bits = ptr, low 32 bits = len
        let out_ptr = (packed_result >> 32) as u32;
        let out_len = (packed_result & 0xFFFF_FFFF) as u32;

        // Validate output size
        if out_len as usize > self.limits.max_output_bytes {
            return Err(WasmError::InvalidOutput {
                plugin: self.metadata.name.clone(),
                reason: format!(
                    "output size {} exceeds limit {}",
                    out_len, self.limits.max_output_bytes
                ),
            });
        }

        let mem = self.get_memory()?;
        let mut output_bytes = vec![0u8; out_len as usize];
        mem.read(&self.store, out_ptr as usize, &mut output_bytes)
            .map_err(|e| WasmError::Trap {
                plugin: self.metadata.name.clone(),
                message: format!("failed to read output from guest memory: {}", e),
            })?;

        // Dealloc output
        let _ = self.dealloc_fn.call(&mut self.store, (out_ptr, out_len));

        Ok(output_bytes)
    }

    fn reset_fuel_and_epoch(&mut self) {
        let _ = self.store.set_fuel(self.limits.max_execution_fuel);
        let epoch_ticks = (self.limits.timeout_ms / 100).max(1);
        self.store.set_epoch_deadline(epoch_ticks);
    }

    fn get_memory(&mut self) -> Result<wasmtime::Memory, WasmError> {
        self.instance
            .get_memory(&mut self.store, "memory")
            .ok_or_else(|| WasmError::MissingExport {
                plugin: self.metadata.name.clone(),
                export: "memory".to_string(),
            })
    }

    fn classify_trap(&self, error: anyhow::Error, context: &str) -> WasmError {
        if let Some(trap) = error.downcast_ref::<wasmtime::Trap>() {
            match trap {
                wasmtime::Trap::OutOfFuel => {
                    return WasmError::FuelExhausted {
                        plugin: self.metadata.name.clone(),
                        fuel_limit: self.limits.max_execution_fuel,
                    };
                }
                wasmtime::Trap::Interrupt => {
                    return WasmError::Timeout {
                        plugin: self.metadata.name.clone(),
                        timeout_ms: self.limits.timeout_ms,
                    };
                }
                _ => {}
            }
        }

        let msg = error.to_string();
        if msg.contains("fuel") {
            WasmError::FuelExhausted {
                plugin: self.metadata.name.clone(),
                fuel_limit: self.limits.max_execution_fuel,
            }
        } else if msg.contains("epoch") || msg.contains("interrupt") {
            WasmError::Timeout {
                plugin: self.metadata.name.clone(),
                timeout_ms: self.limits.timeout_ms,
            }
        } else if msg.contains("memory") && msg.contains("grow") {
            WasmError::MemoryExceeded {
                plugin: self.metadata.name.clone(),
                limit_bytes: self.limits.max_memory_bytes,
            }
        } else {
            WasmError::Trap {
                plugin: self.metadata.name.clone(),
                message: format!("{}: {}", context, msg),
            }
        }
    }

    fn get_typed_func<P: wasmtime::WasmParams, R: wasmtime::WasmResults>(
        store: &mut Store<PluginState>,
        instance: &Instance,
        plugin: &str,
        name: &str,
    ) -> Result<TypedFunc<P, R>, WasmError> {
        instance
            .get_typed_func::<P, R>(store, name)
            .map_err(|_| WasmError::MissingExport {
                plugin: plugin.to_string(),
                export: name.to_string(),
            })
    }

    fn try_get_typed_func<P: wasmtime::WasmParams, R: wasmtime::WasmResults>(
        store: &mut Store<PluginState>,
        instance: &Instance,
        name: &str,
    ) -> Option<TypedFunc<P, R>> {
        instance.get_typed_func::<P, R>(store, name).ok()
    }
}
