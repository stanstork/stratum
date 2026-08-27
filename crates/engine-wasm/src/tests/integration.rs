#[cfg(test)]
mod tests {
    use crate::{
        error::WasmError,
        exchange::{columnar_v1, types::PluginInput},
        registry::{PluginDef, PluginRegistry},
        runtime::{
            engine::{WasmEngine, WasmEngineConfig},
            instance::PluginInstance,
            limits::{HostCapabilities, ResourceLimits},
        },
        schema::{PluginField, PluginType},
    };
    use model::core::value::Value;
    use std::{
        path::{Path, PathBuf},
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
    };
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

        // 8. Build input: a=10.0, b=3.0 (columnar wire needs the schema for columns)
        let mut input = PluginInput::new();
        input.insert("a".into(), Value::Float(10.0));
        input.insert("b".into(), Value::Float(3.0));

        let schema = [
            PluginField {
                name: "a".into(),
                field_type: "f64".into(),
                nullable: false,
            },
            PluginField {
                name: "b".into(),
                field_type: "f64".into(),
                nullable: false,
            },
        ];
        let input_bytes = columnar_v1::serialize_input_batch(&[input], &schema).unwrap();
        println!("Sending columnar input: {} bytes", input_bytes.len());

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

        println!("Raw columnar output: {} bytes", out_bytes.len());

        // 12. Deserialize through our exchange layer
        let outputs = columnar_v1::deserialize_output_batch(&out_bytes, "test_transform").unwrap();
        let output = outputs.into_iter().next().unwrap();
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

        let output = instance.call_transform(&[input]).unwrap().pop().unwrap();
        // Test plugin returns a + b
        assert_eq!(output.value, Value::Float(13.0));
    }

    /// Round-trip every Value type through the real columnar host <-> guest boundary
    /// using the identity `test_echo` plugin. This is the cross-boundary safety
    /// net: any byte-level disagreement between the host and guest codecs shows
    /// up as a changed value here (the host round-trip unit tests only exercise
    /// one side).
    #[tokio::test]
    async fn test_echo_roundtrips_every_type_across_the_boundary() {
        use bigdecimal::BigDecimal;
        use chrono::{NaiveDate, NaiveTime};
        use std::str::FromStr;

        let mut engine = WasmEngine::new(WasmEngineConfig::default()).unwrap();
        let module = engine
            .load_module(&test_plugin_path("test_echo.wasm"))
            .unwrap();
        let mut instance = engine
            .instantiate(
                &module,
                "test_echo".into(),
                HostCapabilities::default(),
                ResourceLimits::for_row_plugins(),
                None,
            )
            .unwrap();

        let date = NaiveDate::from_ymd_opt(2026, 8, 10).unwrap();
        let cases = vec![
            Value::Null,
            Value::Boolean(true),
            Value::Int(-42),
            Value::UInt(u64::MAX),
            Value::Float(3.5),
            Value::String("héllo 🌍".into()),
            Value::Binary(vec![0, 1, 2, 255]),
            Value::Date(date),
            Value::Time {
                value: NaiveTime::from_hms_nano_opt(23, 59, 59, 987_654_321).unwrap(),
                offset_secs: None,
            },
            Value::Timestamp {
                value: date.and_hms_opt(14, 30, 0).unwrap(),
                offset_secs: Some(-3600),
            },
            Value::Uuid(uuid::Uuid::from_u128(
                0xdead_beef_dead_beef_dead_beef_dead_beef,
            )),
            Value::Decimal(BigDecimal::from_str("-12345.6789").unwrap()),
            Value::Json(serde_json::json!({"k": [1, 2, 3]})),
        ];

        // One row at a time so a mismatch names the exact type.
        for expected in &cases {
            let mut input = PluginInput::new();
            input.insert("x".into(), expected.clone());
            let out = instance.call_transform(&[input]).unwrap().pop().unwrap();
            assert_eq!(out.value, *expected, "type did not survive the boundary");
        }

        // A column mixing several *non-null* variants can't use a native tag, so
        // it falls back to the per-cell CELL path.
        let mixed = vec![
            Value::Int(-7),
            Value::String("mixed".into()),
            Value::Boolean(false),
            Value::Null,
            Value::Float(2.25),
        ];
        let inputs: Vec<PluginInput> = mixed
            .iter()
            .map(|v| {
                let mut p = PluginInput::new();
                p.insert("x".into(), v.clone());
                p
            })
            .collect();
        let outs = instance.call_transform(&inputs).unwrap();
        let got: Vec<Value> = outs.into_iter().map(|o| o.value).collect();
        assert_eq!(got, mixed, "mixed-scalar batch did not round-trip via CELL");
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
        let decision = instance.call_evaluate(&[input]).unwrap().pop().unwrap();
        assert!(decision.is_pass());

        // Reject case: value <= 0
        let mut input = PluginInput::new();
        input.insert("value".into(), Value::Int(-1));
        let decision = instance.call_evaluate(&[input]).unwrap().pop().unwrap();
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
        let result = instance.call_transform(&[input]);
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
        let result = instance.call_transform(&[input]);
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

    /// End-to-end host<->guest capability check: the `test_caps` plugin reads a
    /// granted env var, reads a file through the granted fs preopen, and keeps a
    /// per-instance kv counter. Each row's output encodes all three, so a single
    /// assertion proves env + fs + kv work across the real boundary.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_capabilities_env_fs_http_kv_granted() {
        let dir = std::env::temp_dir().join("stratum-caps-itest");
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("probe.txt");
        std::fs::write(&file, "filecontents\n").unwrap();

        // A loopback HTTP server the plugin will GET (one response per row).
        let (url, server) = serve_http(2, b"pong");

        let mut engine = WasmEngine::new(WasmEngineConfig::default()).unwrap();
        let module = engine
            .load_module(&test_plugin_path("test_caps.wasm"))
            .unwrap();

        let caps = HostCapabilities {
            env: vec![
                ("CAPS_ENV".into(), "envvalue".into()),
                ("CAPS_FILE".into(), file.to_string_lossy().into_owned()),
                ("CAPS_HTTP_URL".into(), url),
            ],
            fs_read: vec![dir.clone()],
            key_value_store: true,
            metrics: true,
            http_client: true,
            http_allowed_hosts: vec!["127.0.0.1".into()],
            ..HostCapabilities::default()
        };
        let mut instance = engine
            .instantiate(
                &module,
                "test_caps".into(),
                caps,
                ResourceLimits::for_io_plugins(),
                None,
            )
            .unwrap();

        // WASI fs ops block on Tokio internally; keep them off the async worker.
        let call = |instance: &mut PluginInstance| {
            let mut input = PluginInput::new();
            input.insert("seed".into(), Value::String("row".into()));
            tokio::task::block_in_place(|| instance.call_transform(&[input]))
                .unwrap()
                .pop()
                .unwrap()
                .value
        };

        // Row 1: env + fs + http resolved; kv counter starts at 1.
        assert_eq!(
            call(&mut instance),
            Value::String("kv=1;env=envvalue;fs=filecontents;http=200:pong".into())
        );
        // Row 2: same instance -> the kv counter persists and increments.
        assert_eq!(
            call(&mut instance),
            Value::String("kv=2;env=envvalue;fs=filecontents;http=200:pong".into())
        );

        server.join().ok();
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A loopback HTTP/1.1 server that answers `responses` requests with `body`,
    /// then exits. Returns its URL and the join handle.
    fn serve_http(responses: usize, body: &'static [u8]) -> (String, std::thread::JoinHandle<()>) {
        use std::io::{Read, Write};
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = std::thread::spawn(move || {
            for _ in 0..responses {
                if let Ok((mut stream, _)) = listener.accept() {
                    let mut buf = [0u8; 1024];
                    let _ = stream.read(&mut buf);
                    let head = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = stream.write_all(head.as_bytes());
                    let _ = stream.write_all(body);
                    let _ = stream.flush();
                }
            }
        });
        (format!("http://{addr}/"), handle)
    }

    /// With no capabilities granted, the same plugin degrades cleanly: env/fs
    /// read as "-", and the kv counter never persists (denied `kv_set` is a
    /// no-op, so every row starts from 1).
    #[tokio::test(flavor = "multi_thread")]
    async fn test_capabilities_denied_are_inert() {
        let mut engine = WasmEngine::new(WasmEngineConfig::default()).unwrap();
        let module = engine
            .load_module(&test_plugin_path("test_caps.wasm"))
            .unwrap();
        let mut instance = engine
            .instantiate(
                &module,
                "test_caps".into(),
                HostCapabilities::default(),
                ResourceLimits::for_io_plugins(),
                None,
            )
            .unwrap();

        let call = |instance: &mut crate::runtime::instance::PluginInstance| {
            let mut input = PluginInput::new();
            input.insert("seed".into(), Value::String("row".into()));
            tokio::task::block_in_place(|| instance.call_transform(&[input]))
                .unwrap()
                .pop()
                .unwrap()
                .value
        };

        assert_eq!(
            call(&mut instance),
            Value::String("kv=1;env=-;fs=-;http=-".into())
        );
        assert_eq!(
            call(&mut instance),
            Value::String("kv=1;env=-;fs=-;http=-".into())
        );
    }

    /// Minimal subscriber that flags whether any event was emitted on the
    /// `plugin::metrics` target - enough to assert a metric fired without pulling
    /// in a logging test dependency.
    struct MetricSpy(Arc<AtomicBool>);

    impl tracing::Subscriber for MetricSpy {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            if event.metadata().target() == "plugin::metrics" {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
    }

    /// The `metrics` capability actually emits: granted -> a `plugin::metrics`
    /// event fires per row; denied -> nothing. The plugin runs on this thread
    /// (via `block_in_place`), so a thread-local subscriber reliably observes it.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_capabilities_metrics_emitted_only_when_granted() {
        let observe = |metrics_granted: bool| -> bool {
            let mut engine = WasmEngine::new(WasmEngineConfig::default()).unwrap();
            let module = engine
                .load_module(&test_plugin_path("test_caps.wasm"))
                .unwrap();
            let caps = HostCapabilities {
                metrics: metrics_granted,
                key_value_store: true,
                ..HostCapabilities::default()
            };
            let mut instance = engine
                .instantiate(
                    &module,
                    "test_caps".into(),
                    caps,
                    ResourceLimits::for_io_plugins(),
                    None,
                )
                .unwrap();

            let seen = Arc::new(AtomicBool::new(false));
            let spy = MetricSpy(seen.clone());
            tracing::subscriber::with_default(spy, || {
                let mut input = PluginInput::new();
                input.insert("seed".into(), Value::String("row".into()));
                tokio::task::block_in_place(|| instance.call_transform(&[input])).unwrap();
            });
            seen.load(Ordering::SeqCst)
        };

        assert!(
            observe(true),
            "granted metrics must emit a plugin::metrics event"
        );
        assert!(!observe(false), "denied metrics must emit nothing");
    }
}
