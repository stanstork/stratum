#[cfg(test)]
mod tests {
    use crate::{
        error::WasmError,
        exchange::{json_v1, types::PluginInput},
        registry::{PluginDef, PluginRegistry},
        runtime::{
            engine::{WasmEngine, WasmEngineConfig},
            limits::{HostCapabilities, ResourceLimits},
        },
        schema::PluginType,
    };
    use model::core::value::Value;
    use std::path::{Path, PathBuf};
    use wasmtime::{Config, Engine, Linker, Module, Store};
    use wasmtime_wasi::preview1;

    struct WasiState {
        wasi_ctx: wasmtime_wasi::preview1::WasiP1Ctx,
    }

    fn test_plugin_path(name: &str) -> PathBuf {
        // Test fixtures compiled to target/wasm32-wasip1/release/
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("tests")
            .join("fixtures")
            .join(name)
    }

    #[tokio::test]
    async fn test_transform_raw_call() {
        // 1. Boot wasmtime
        let mut config = Config::new();
        config.consume_fuel(true);
        let engine = Engine::new(&config).unwrap();

        // 2. Link WASI (the plugin is wasm32-wasip1)
        let mut linker: Linker<WasiState> = Linker::new(&engine);
        preview1::add_to_linker_sync(&mut linker, |s: &mut WasiState| &mut s.wasi_ctx).unwrap();

        for name in ["log_debug", "log_info", "log_warn", "log_error"] {
            linker
                .func_wrap("stratum", name, |_ptr: u32, _len: u32| {})
                .unwrap();
        }

        // 3. Create store with fuel
        let wasi_ctx = wasmtime_wasi::WasiCtxBuilder::new()
            .inherit_stdio()
            .build_p1();
        let mut store = Store::new(&engine, WasiState { wasi_ctx });
        store.set_fuel(10_000_000).unwrap();

        // 4. Load and instantiate the plugin
        let path = test_plugin_path("test_transform.wasm");
        println!("Loading plugin from: {}", path.display());
        let module = Module::from_file(&engine, &path).unwrap();
        let instance = linker.instantiate(&mut store, &module).unwrap();

        // 5. Resolve ABI exports
        let alloc_fn = instance
            .get_typed_func::<u32, u32>(&mut store, "__stratum_alloc")
            .unwrap();
        let dealloc_fn = instance
            .get_typed_func::<(u32, u32), ()>(&mut store, "__stratum_dealloc")
            .unwrap();
        let metadata_fn = instance
            .get_typed_func::<(), u64>(&mut store, "__stratum_metadata")
            .unwrap();
        // SDK-generated __stratum_initialize takes (config_ptr, config_len)
        // so plugins can receive runtime configuration. Pass (0, 0) for none.
        let init_fn = instance
            .get_typed_func::<(u32, u32), u32>(&mut store, "__stratum_initialize")
            .unwrap();
        let transform_fn = instance
            .get_typed_func::<(u32, u32), u64>(&mut store, "__stratum_transform")
            .unwrap();
        let memory = instance.get_memory(&mut store, "memory").unwrap();

        // 6. Read metadata
        let packed = metadata_fn.call(&mut store, ()).unwrap();
        let meta_ptr = (packed >> 32) as u32;
        let meta_len = (packed & 0xFFFF_FFFF) as u32;
        let mut meta_bytes = vec![0u8; meta_len as usize];
        memory
            .read(&store, meta_ptr as usize, &mut meta_bytes)
            .unwrap();
        let meta_str = std::str::from_utf8(&meta_bytes).unwrap();
        println!("Plugin metadata: {}", meta_str);

        // 7. Initialize (no config)
        let status = init_fn.call(&mut store, (0, 0)).unwrap();
        assert_eq!(status, 0, "initialize failed");
        println!("Plugin initialized (status={})", status);

        // 8. Build input: a=10.0, b=3.0
        let mut input = PluginInput::new();
        input.insert("a".into(), Value::Float(10.0));
        input.insert("b".into(), Value::Float(3.0));
        let input_bytes = json_v1::serialize_input(&input, &[]).unwrap();
        println!(
            "Sending input: {}",
            std::str::from_utf8(&input_bytes).unwrap()
        );

        // 9. Write input into guest memory
        let input_len = input_bytes.len() as u32;
        let input_ptr = alloc_fn.call(&mut store, input_len).unwrap();
        memory
            .write(&mut store, input_ptr as usize, &input_bytes)
            .unwrap();

        // 10. Call transform
        let result_packed = transform_fn
            .call(&mut store, (input_ptr, input_len))
            .unwrap();
        let _ = dealloc_fn.call(&mut store, (input_ptr, input_len));

        // 11. Read output from guest memory
        let out_ptr = (result_packed >> 32) as u32;
        let out_len = (result_packed & 0xFFFF_FFFF) as u32;
        let mut out_bytes = vec![0u8; out_len as usize];
        memory
            .read(&store, out_ptr as usize, &mut out_bytes)
            .unwrap();
        let _ = dealloc_fn.call(&mut store, (out_ptr, out_len));

        let out_str = std::str::from_utf8(&out_bytes).unwrap();
        println!("Raw output: {}", out_str);

        // 12. Deserialize through our exchange layer
        let output = json_v1::deserialize_output(&out_bytes, "test_transform").unwrap();
        println!("Deserialized: {:?}", output.value);

        assert_eq!(output.value, Value::Float(13.0));
        println!("10.0 + 3.0 = 13.0 — WASM plugin works!");
    }

    #[tokio::test]
    async fn test_load_and_inspect_transform_plugin() {
        let mut engine = WasmEngine::new(WasmEngineConfig::default()).unwrap();
        let module = engine
            .load_module(&test_plugin_path("test_transform.wasm"))
            .unwrap();

        let instance = engine
            .instantiate(
                &module,
                "test_transform".into(),
                HostCapabilities::default(),
                ResourceLimits::for_row_plugins(),
                None,
            )
            .unwrap();

        let meta = instance.metadata();
        assert_eq!(meta.name, "test_transform");
        assert_eq!(meta.plugin_type, PluginType::Transform);
        assert_eq!(meta.output_type.as_deref(), Some("f64"));
        assert_eq!(meta.input_schema.len(), 2);
    }

    #[tokio::test]
    async fn test_transform_call() {
        let mut engine = WasmEngine::new(WasmEngineConfig::default()).unwrap();
        let module = engine
            .load_module(&test_plugin_path("test_transform.wasm"))
            .unwrap();

        let mut instance = engine
            .instantiate(
                &module,
                "test_transform".into(),
                HostCapabilities::default(),
                ResourceLimits::for_row_plugins(),
                None,
            )
            .unwrap();

        let mut input = PluginInput::new();
        input.insert("a".into(), Value::Float(10.0));
        input.insert("b".into(), Value::Float(3.0));

        let output = instance.call_transform(&input).unwrap();
        // Test plugin returns a + b
        assert_eq!(output.value, Value::Float(13.0));
    }

    #[tokio::test]
    async fn test_filter_pass_and_reject() {
        let mut engine = WasmEngine::new(WasmEngineConfig::default()).unwrap();
        let module = engine
            .load_module(&test_plugin_path("test_filter.wasm"))
            .unwrap();
        let mut instance = engine
            .instantiate(
                &module,
                "test_filter".into(),
                HostCapabilities::default(),
                ResourceLimits::for_row_plugins(),
                None,
            )
            .unwrap();

        // Pass case: value > 0
        let mut input = PluginInput::new();
        input.insert("value".into(), Value::Int(42));
        let decision = instance.call_evaluate(&input).unwrap();
        assert!(decision.is_pass());

        // Reject case: value <= 0
        let mut input = PluginInput::new();
        input.insert("value".into(), Value::Int(-1));
        let decision = instance.call_evaluate(&input).unwrap();
        assert!(!decision.is_pass());
    }

    #[tokio::test]
    async fn test_fuel_exhaustion() {
        let mut engine = WasmEngine::new(WasmEngineConfig::default()).unwrap();
        let module = engine
            .load_module(&test_plugin_path("test_infinite_loop.wasm"))
            .unwrap();

        let limits = ResourceLimits {
            // Enough fuel to initialize and write the input, but the spin
            // loop will burn through this in microseconds.
            max_execution_fuel: 200_000,
            ..ResourceLimits::for_row_plugins()
        };
        let mut instance = engine
            .instantiate(
                &module,
                "test_loop".into(),
                HostCapabilities::default(),
                limits,
                None,
            )
            .unwrap();

        let input = PluginInput::new();
        let result = instance.call_transform(&input);
        assert!(matches!(result, Err(WasmError::FuelExhausted { .. })));
    }

    #[tokio::test]
    async fn test_memory_limit() {
        let mut engine = WasmEngine::new(WasmEngineConfig::default()).unwrap();
        let module = engine
            .load_module(&test_plugin_path("test_memory_hog.wasm"))
            .unwrap();

        let limits = ResourceLimits {
            max_memory_bytes: 4 * 1024 * 1024,
            ..ResourceLimits::for_row_plugins()
        };
        let mut instance = engine
            .instantiate(
                &module,
                "test_mem".into(),
                HostCapabilities::default(),
                limits,
                None,
            )
            .unwrap();

        let input = PluginInput::new();
        let result = instance.call_transform(&input);
        assert!(matches!(
            result,
            Err(WasmError::MemoryExceeded { .. }) | Err(WasmError::Trap { .. })
        ));
    }

    #[tokio::test]
    async fn test_plugin_not_found() {
        let mut engine = WasmEngine::new(WasmEngineConfig::default()).unwrap();
        let result = engine.load_module(Path::new("/nonexistent/plugin.wasm"));
        assert!(matches!(result, Err(WasmError::PluginNotFound { .. })));
    }

    #[tokio::test]
    async fn test_registry_load_and_instantiate() {
        let mut registry = PluginRegistry::new(&WasmEngineConfig::default()).unwrap();

        let def = PluginDef {
            name: "adder".into(),
            path: test_plugin_path("test_transform.wasm"),
            capabilities: HostCapabilities::default(),
            limits: ResourceLimits::for_row_plugins(),
            config_json: None,
        };
        registry.load(&def).unwrap();
        assert!(registry.is_loaded("adder"));

        let instance = registry.instantiate("adder").unwrap();
        assert_eq!(instance.plugin_name(), "test_transform");
    }
}
