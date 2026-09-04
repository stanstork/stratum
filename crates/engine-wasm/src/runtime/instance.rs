use std::collections::HashMap;

use crate::{
    error::WasmError,
    exchange::{
        ExchangeFormat, columnar_v1, json_v1,
        types::{FilterDecision, PluginBatch, PluginInput, PluginOutput, SourcePage, WriteResult},
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

/// Per-call resource budget.
struct CallBudget {
    fuel: u64,
    max_output_bytes: usize,
    epoch_ticks: u64,
}

/// Wall-clock ceiling (in 100ms epoch ticks) for a single batch call.
const MAX_EPOCH_TICKS: u64 = 300;

/// Host-memory ceiling for a single batch's output, regardless of batch size, so
/// a misbehaving guest can't make the host allocate an unbounded output buffer.
const MAX_OUTPUT_BYTES: usize = 256 * 1024 * 1024; // 256 MB

macro_rules! require_export {
    ($self:ident, $func_field:ident, $export_name:expr) => {
        $self
            .$func_field
            .clone()
            .ok_or_else(|| WasmError::MissingExport {
                plugin: $self.metadata.name.clone(),
                export: $export_name.to_string(),
            })?
    };
}

macro_rules! process_batch {
    (
        $self:ident,
        $func_field:ident,
        $export_name:expr,
        $input_len:expr,
        $serialize:expr,
        $deserialize:expr
    ) => {{
        let func = require_export!($self, $func_field, $export_name);
        let is_columnar = $self.metadata.exchange_format == ExchangeFormat::ColumnarV1;

        let input_bytes = $serialize(is_columnar)?;
        let budget = $self.batch_budget($input_len);
        let output_bytes = $self.call_data_fn(func, &input_bytes, &budget)?;
        let outputs = $deserialize(is_columnar, &output_bytes)?;

        if outputs.len() != $input_len {
            let action = $export_name
                .strip_prefix("__paganel_")
                .unwrap_or($export_name);
            return Err(WasmError::InvalidOutput {
                plugin: $self.metadata.name.clone(),
                reason: format!(
                    "{} returned {} results for {} inputs",
                    action,
                    outputs.len(),
                    $input_len
                ),
            });
        }

        Ok(outputs)
    }};
}

macro_rules! exec_hook {
    ($self:ident, $func_field:ident, $hook_name:expr, $args:expr) => {{
        let Some(func) = $self.$func_field.clone() else {
            return Ok(());
        };

        $self.reset_fuel_and_epoch();
        let status = func
            .call(&mut $self.store, $args)
            .map_err(|e| $self.classify_trap(e, $hook_name))?;

        if status != 0 {
            return Err(WasmError::PluginError {
                plugin: $self.metadata.name.clone(),
                message: format!("{}() returned status code {}", $hook_name, status),
            });
        }
        Ok(())
    }};
}

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
        let state = PluginState::new(plugin_name.clone(), capabilities, &limits)?;
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
            "__paganel_alloc",
        )?;
        let dealloc_fn = Self::get_typed_func::<(u32, u32), ()>(
            &mut store,
            &instance,
            &plugin_name,
            "__paganel_dealloc",
        )?;

        // Resolve role-specific exports (optional - determined by which ones exist)
        let transform_fn = Self::try_get_typed_func(&mut store, &instance, "__paganel_transform");
        let evaluate_fn = Self::try_get_typed_func(&mut store, &instance, "__paganel_evaluate");
        let read_page_fn = Self::try_get_typed_func(&mut store, &instance, "__paganel_read_page");
        let write_batch_fn =
            Self::try_get_typed_func(&mut store, &instance, "__paganel_write_batch");
        let prepare_fn = Self::try_get_typed_func(&mut store, &instance, "__paganel_prepare");
        let finalize_fn = Self::try_get_typed_func(&mut store, &instance, "__paganel_finalize");

        // Load metadata
        let metadata_fn = Self::get_typed_func::<(), u64>(
            &mut store,
            &instance,
            &plugin_name,
            "__paganel_metadata",
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
            "__paganel_initialize",
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

        info!(plugin = %plugin_name, plugin_type = ?metadata.plugin_type, version = %metadata.version, "plugin initialized");

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

    /// Call a transform plugin over a whole batch in one crossing. The batch is the
    /// unit of the ABI - the plugin receives every input and returns one output per
    /// input, in order.
    pub fn call_transform(
        &mut self,
        inputs: &[PluginInput],
    ) -> Result<Vec<PluginOutput>, WasmError> {
        process_batch!(
            self,
            transform_fn,
            "__paganel_transform",
            inputs.len(),
            |is_columnar| {
                if is_columnar {
                    columnar_v1::serialize_input_batch(inputs, &self.metadata.input_schema)
                } else {
                    json_v1::serialize_input_batch(inputs, &self.metadata.input_schema)
                }
            },
            |is_columnar, bytes: &[u8]| {
                if is_columnar {
                    columnar_v1::deserialize_output_batch(bytes, &self.metadata.name)
                } else {
                    json_v1::deserialize_output_flat(
                        bytes,
                        &self.metadata.name,
                        self.metadata.output_type.as_deref(),
                    )
                }
            }
        )
    }

    /// Like `call_transform`, but builds the plugin input straight from source
    /// records via `mapping` (plugin field -> source column) - no per-row
    /// `PluginInput`/HashMap. Column resolution is hoisted out of the row loop.
    pub fn call_transform_records(
        &mut self,
        rows: &[Record],
        mapping: &HashMap<String, String>,
    ) -> Result<Vec<PluginOutput>, WasmError> {
        process_batch!(
            self,
            transform_fn,
            "__paganel_transform",
            rows.len(),
            |is_columnar| {
                if is_columnar {
                    columnar_v1::serialize_input_from_records(
                        rows,
                        &self.metadata.input_schema,
                        mapping,
                    )
                } else {
                    json_v1::serialize_input_from_records(
                        rows,
                        &self.metadata.input_schema,
                        mapping,
                    )
                }
            },
            |is_columnar, bytes: &[u8]| {
                if is_columnar {
                    columnar_v1::deserialize_output_batch(bytes, &self.metadata.name)
                } else {
                    json_v1::deserialize_output_flat(
                        bytes,
                        &self.metadata.name,
                        self.metadata.output_type.as_deref(),
                    )
                }
            }
        )
    }

    /// Call a filter plugin over a whole batch in one crossing. Returns one
    /// pass/reject decision per input, in order.
    pub fn call_evaluate(
        &mut self,
        inputs: &[PluginInput],
    ) -> Result<Vec<FilterDecision>, WasmError> {
        process_batch!(
            self,
            evaluate_fn,
            "__paganel_evaluate",
            inputs.len(),
            |is_columnar| {
                if is_columnar {
                    columnar_v1::serialize_input_batch(inputs, &self.metadata.input_schema)
                } else {
                    json_v1::serialize_input_batch(inputs, &self.metadata.input_schema)
                }
            },
            |is_columnar, bytes: &[u8]| {
                if is_columnar {
                    columnar_v1::deserialize_filter_decision_batch(bytes, &self.metadata.name)
                } else {
                    json_v1::deserialize_filter_decision_batch(bytes, &self.metadata.name)
                }
            }
        )
    }

    /// Like `call_evaluate`, but builds the plugin input straight from source
    /// records via `mapping` (plugin field -> source column) - no per-row
    /// `PluginInput`/HashMap. This is the batch-native filter path.
    pub fn call_evaluate_records(
        &mut self,
        rows: &[Record],
        mapping: &HashMap<String, String>,
    ) -> Result<Vec<FilterDecision>, WasmError> {
        process_batch!(
            self,
            evaluate_fn,
            "__paganel_evaluate",
            rows.len(),
            |is_columnar| {
                if is_columnar {
                    columnar_v1::serialize_input_from_records(
                        rows,
                        &self.metadata.input_schema,
                        mapping,
                    )
                } else {
                    json_v1::serialize_input_from_records(
                        rows,
                        &self.metadata.input_schema,
                        mapping,
                    )
                }
            },
            |is_columnar, bytes: &[u8]| {
                if is_columnar {
                    columnar_v1::deserialize_filter_decision_batch(bytes, &self.metadata.name)
                } else {
                    json_v1::deserialize_filter_decision_batch(bytes, &self.metadata.name)
                }
            }
        )
    }

    /// Call a source plugin's read_page. The host treats the cursor as opaque
    /// and round-trips it verbatim to the plugin.
    pub fn call_read_page(
        &mut self,
        cursor: Option<&str>,
        _batch_size: usize,
    ) -> Result<SourcePage, WasmError> {
        let func = require_export!(self, read_page_fn, "__paganel_read_page");

        let input_bytes = json_v1::serialize_cursor(cursor)?;
        // The cursor is a single small payload; the page the plugin produces is
        // bounded by its own limits. Use the base (unscaled) budget.
        let budget = self.batch_budget(1);
        let output_bytes = self.call_data_fn(func, &input_bytes, &budget)?;

        json_v1::deserialize_source_page(&output_bytes, &self.metadata.name)
    }

    /// Call a sink plugin's write_batch. Returns how many rows the plugin
    /// reports as committed. The destination (table / endpoint / file) is
    /// supplied to the plugin via its config at init time, so the wire
    /// payload carries only the records.
    pub fn call_write_batch(&mut self, rows: &[Record]) -> Result<WriteResult, WasmError> {
        let func = require_export!(self, write_batch_fn, "__paganel_write_batch");

        let batch = PluginBatch {
            records: rows.to_vec(),
        };
        let input_bytes = json_v1::serialize_batch(&batch)?;

        // write_batch processes a whole batch in one crossing, like transform -
        // scale the budget with the row count.
        let budget = self.batch_budget(rows.len());
        let output_bytes = self.call_data_fn(func, &input_bytes, &budget)?;

        json_v1::deserialize_write_result(&output_bytes, &self.metadata.name)
    }

    /// Call a sink plugin's prepare hook (`__paganel_prepare`). Invoked once
    /// before the first batch so the plugin can open connections, create staging tables, etc.
    pub fn call_prepare(&mut self) -> Result<(), WasmError> {
        exec_hook!(self, prepare_fn, "prepare", (0, 0))
    }

    /// Call a sink plugin's finalize hook (`__paganel_finalize`). Invoked once
    /// after the final batch so the plugin can flush buffers or commit.
    pub fn call_finalize(&mut self) -> Result<(), WasmError> {
        exec_hook!(self, finalize_fn, "finalize", ())
    }

    pub fn read_metadata(
        engine: &Engine,
        linker: &Linker<PluginState>,
        module: &Module,
    ) -> Result<PluginMetadata, WasmError> {
        let limits = ResourceLimits::for_io_plugins();
        let state = PluginState::new("<inspect>".into(), HostCapabilities::default(), &limits)?;

        let mut store = Store::new(engine, state);
        store.limiter(|s| s);
        let _ = store.set_fuel(limits.max_execution_fuel);

        // The engine enables epoch interruption, so a deadline must be set or
        // the first epoch check traps (default deadline is 0).
        store.epoch_deadline_trap();
        store.set_epoch_deadline((limits.timeout_ms / 100).max(1));

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
            "__paganel_dealloc",
        )?;

        let meta_fn = Self::get_typed_func::<(), u64>(
            &mut store,
            &instance,
            "<inspect>",
            "__paganel_metadata",
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

        call_result.map_err(|e| WasmError::Trap {
            plugin: plugin.to_string(),
            message: format!("call failed: {e}"),
        })
    }

    /// Generic data call: write input bytes to guest, call function, read output bytes.
    /// `budget` sizes fuel / output ceiling / wall-clock for this specific call,
    /// scaled to the batch it carries.
    fn call_data_fn(
        &mut self,
        func: TypedFunc<(u32, u32), u64>,
        input_bytes: &[u8],
        budget: &CallBudget,
    ) -> Result<Vec<u8>, WasmError> {
        self.apply_budget(budget);

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

        // Validate output size against this call's (batch-scaled) ceiling.
        if out_len as usize > budget.max_output_bytes {
            return Err(WasmError::InvalidOutput {
                plugin: self.metadata.name.clone(),
                reason: format!(
                    "output size {} exceeds limit {}",
                    out_len, budget.max_output_bytes
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

    /// Budget for a call carrying `rows` rows. The instance's configured limits
    /// are the *per-row* rate; the batch gets that rate times the row count.
    fn batch_budget(&self, rows: usize) -> CallBudget {
        let n = rows.max(1) as u64;
        let base_ticks = (self.limits.timeout_ms / 100).max(1);
        CallBudget {
            fuel: self.limits.max_execution_fuel.saturating_mul(n),
            max_output_bytes: self
                .limits
                .max_output_bytes
                .saturating_mul(rows.max(1))
                .min(MAX_OUTPUT_BYTES),
            epoch_ticks: base_ticks.saturating_mul(n).min(MAX_EPOCH_TICKS),
        }
    }

    fn apply_budget(&mut self, budget: &CallBudget) {
        let _ = self.store.set_fuel(budget.fuel);
        self.store.set_epoch_deadline(budget.epoch_ticks);
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
