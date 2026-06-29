use crate::{
    error::WasmError,
    runtime::{
        host_functions::{PluginState, link_host_functions},
        instance::PluginInstance,
        limits::{HostCapabilities, ResourceLimits},
    },
    schema::PluginMetadata,
};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};
use tracing::{debug, info};
use wasmtime::{Engine, Linker, Module};

#[derive(Debug, Clone)]
pub struct WasmEngineConfig {
    /// Enable AOT compilation caching to disk.
    pub cache_compiled_modules: bool,
    /// Directory for cached compiled modules. Default: ~/.stratum/wasm_cache/
    pub cache_dir: Option<PathBuf>,
    /// Enable parallel compilation of WASM modules.
    pub parallel_compilation: bool,
}

impl Default for WasmEngineConfig {
    fn default() -> Self {
        Self {
            cache_compiled_modules: true,
            cache_dir: None,
            parallel_compilation: true,
        }
    }
}

/// WASM runtime engine. Compiles and caches modules.
pub struct WasmEngine {
    engine: Engine,
    linker: Linker<PluginState>,
    module_cache: HashMap<PathBuf, Arc<Module>>,
}

impl WasmEngine {
    pub fn new(config: WasmEngineConfig) -> Result<Self, WasmError> {
        let mut wasm_config = wasmtime::Config::new();
        wasm_config.consume_fuel(true);
        wasm_config.epoch_interruption(true);
        wasm_config.parallel_compilation(config.parallel_compilation);

        if config.cache_compiled_modules {
            // TODO: honor `config.cache_dir` via a custom CacheConfig.
            if let Ok(cache) = wasmtime::Cache::new(wasmtime::CacheConfig::default()) {
                wasm_config.cache(Some(cache));
            }
        }

        let engine = Engine::new(&wasm_config).map_err(|e| WasmError::CompilationFailed {
            path: PathBuf::from("<engine>"),
            source: e,
        })?;

        // Start epoch ticker in background
        let engine_clone = engine.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));
            loop {
                interval.tick().await;
                engine_clone.increment_epoch();
            }
        });

        // Create linker with host functions
        let mut linker = Linker::new(&engine);
        link_host_functions(&mut linker)?;

        // Link WASI (for stdout/stderr capture)
        wasmtime_wasi::preview1::add_to_linker_sync(&mut linker, |state: &mut PluginState| {
            &mut state.wasi_ctx
        })
        .map_err(|e| WasmError::HostFunctionError {
            function: "wasi".to_string(),
            message: e.to_string(),
        })?;

        info!("WASM engine initialized");

        Ok(Self {
            engine,
            linker,
            module_cache: HashMap::new(),
        })
    }

    pub fn load_module(&mut self, path: &Path) -> Result<Arc<Module>, WasmError> {
        let canonical = path.canonicalize().map_err(|_| WasmError::PluginNotFound {
            path: path.to_path_buf(),
        })?;

        if let Some(module) = self.module_cache.get(&canonical) {
            debug!(path = %canonical.display(), "using cached WASM module");
            return Ok(Arc::clone(module));
        }

        info!(path = %canonical.display(), "compiling WASM module");

        let module = Module::from_file(&self.engine, &canonical).map_err(|e| {
            WasmError::CompilationFailed {
                path: canonical.clone(),
                source: e,
            }
        })?;

        let module = Arc::new(module);
        self.module_cache.insert(canonical, Arc::clone(&module));
        Ok(module)
    }

    pub fn instantiate(
        &self,
        module: &Module,
        plugin_name: String,
        capabilities: HostCapabilities,
        limits: ResourceLimits,
        config_json: Option<&[u8]>,
    ) -> Result<PluginInstance, WasmError> {
        PluginInstance::new(
            &self.engine,
            &self.linker,
            module,
            plugin_name,
            capabilities,
            limits,
            config_json,
        )
    }

    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    pub fn read_metadata(&self, module: &Module) -> Result<PluginMetadata, WasmError> {
        PluginInstance::read_metadata(&self.engine, &self.linker, module)
    }
}
