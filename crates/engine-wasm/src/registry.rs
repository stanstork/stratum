use crate::{
    error::WasmError,
    runtime::{
        engine::{WasmEngine, WasmEngineConfig},
        instance::PluginInstance,
        limits::{HostCapabilities, ResourceLimits},
    },
    schema::PluginMetadata,
};
use model::{
    core::types::Type,
    execution::{pipeline::Pipeline, plugin::PluginDecl},
};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};
use tracing::info;
use wasmtime::Module;

/// Plugin definition from SMQL configuration.
#[derive(Debug, Clone)]
pub struct PluginDef {
    pub name: String,
    pub path: PathBuf,
    pub capabilities: HostCapabilities,
    pub limits: ResourceLimits,
    /// Plugin-specific config as JSON bytes (from SMQL `config { }` block).
    pub config_json: Option<Vec<u8>>,
}

impl PluginDef {
    pub fn from_path(name: &str, path: &Path) -> PluginDef {
        PluginDef {
            name: name.into(),
            path: path.to_path_buf(),
            capabilities: HostCapabilities::default(), // logging on; http/kv/metrics off
            // JS needs generous fuel/memory; for_io_plugins (100M / 128MB) covers QuickJS boot.
            limits: ResourceLimits::for_io_plugins(),
            config_json: None,
        }
    }
}

/// Host capabilities from an SMQL `plugin { ... }` declaration.
pub fn caps_from_decl(decl: &PluginDecl) -> HostCapabilities {
    HostCapabilities {
        http_client: decl.allow_http,
        key_value_store: decl.allow_kv,
        logging: decl.allow_log,
        metrics: decl.allow_metrics,
    }
}

/// Resource limits to run a plugin with: start from the runtime/role-appropriate
/// ceiling the plugin's own metadata suggests (`suggested_limits` - JS/IO plugins
/// get the generous `for_io_plugins` budget QuickJS boot needs, native row plugins
/// get the tight `for_row_plugins` budget), then let any explicit SMQL override win.
pub fn resolve_limits(meta: &PluginMetadata, decl: &PluginDecl) -> ResourceLimits {
    let mut limits = meta.suggested_limits();
    if let Some(m) = decl.memory_limit_bytes {
        limits.max_memory_bytes = m as usize;
    }
    if let Some(f) = decl.fuel_limit {
        limits.max_execution_fuel = f;
    }
    if let Some(t) = decl.timeout_ms {
        limits.timeout_ms = t;
    }
    limits
}

/// Build a registry pre-loaded with every plugin referenced in the plan.
/// Shared by `DagExecutor` (apply) and `ReportBuilder` (plan --sample) so
/// both paths instantiate plugins identically.
pub fn load_registry(decls: &[PluginDecl]) -> Result<Arc<PluginRegistry>, WasmError> {
    let mut registry = PluginRegistry::new(&WasmEngineConfig::default())?;
    for decl in decls {
        registry.load_decl(decl)?;
    }
    Ok(Arc::new(registry))
}

/// Per-run plugin registry. Thread-safe module caching, per-pipeline instance creation.
pub struct PluginRegistry {
    engine: WasmEngine,
    /// Compiled modules keyed by plugin name.
    modules: HashMap<String, (Arc<Module>, PluginDef)>,
}

impl PluginRegistry {
    pub fn new(config: &WasmEngineConfig) -> Result<Self, WasmError> {
        Ok(Self {
            engine: WasmEngine::new(config.clone())?,
            modules: HashMap::new(),
        })
    }

    /// Load and compile a plugin definition. Idempotent  second call for same name is a no-op.
    pub fn load(&mut self, def: &PluginDef) -> Result<(), WasmError> {
        if self.modules.contains_key(&def.name) {
            return Ok(());
        }

        let module = self.engine.load_module(&def.path)?;
        self.modules.insert(def.name.clone(), (module, def.clone()));
        info!(plugin = %def.name, path = %def.path.display(), "Plugin loaded");
        Ok(())
    }

    /// Load a plugin straight from its SMQL declaration. Compiles the module,
    /// reads its metadata, and sizes resource limits from the plugin's runtime
    /// hint (so JS plugins get the fuel QuickJS boot needs without the author
    /// having to spell it out) with explicit SMQL overrides applied on top.
    pub fn load_decl(&mut self, decl: &PluginDecl) -> Result<(), WasmError> {
        if self.modules.contains_key(&decl.name) {
            return Ok(());
        }

        let module = self.engine.load_module(&decl.path)?;
        let meta = self.engine.read_metadata(&module)?;
        let def = PluginDef {
            name: decl.name.clone(),
            path: decl.path.clone(),
            capabilities: caps_from_decl(decl),
            limits: resolve_limits(&meta, decl),
            config_json: decl.config_json.clone(),
        };
        self.modules.insert(decl.name.clone(), (module, def));
        info!(plugin = %decl.name, path = %decl.path.display(), "Plugin loaded");
        Ok(())
    }

    /// Create a fresh instance for a pipeline. Each pipeline gets its own isolated instance.
    pub fn instantiate(&self, name: &str) -> Result<PluginInstance, WasmError> {
        let (module, def) = self
            .modules
            .get(name)
            .ok_or_else(|| WasmError::PluginNotLoaded {
                name: name.to_string(),
            })?;

        self.engine.instantiate(
            module,
            def.name.clone(),
            def.capabilities.clone(),
            def.limits.clone(),
            def.config_json.as_deref(),
        )
    }

    pub fn is_loaded(&self, name: &str) -> bool {
        self.modules.contains_key(name)
    }

    pub fn metadata(&self, name: &str) -> Result<PluginMetadata, WasmError> {
        Ok(self.instantiate(name)?.metadata().clone())
    }
}

/// Resolve the destination columns produced by a pipeline's plugin transforms
/// (`select { col = plugin.x({...}) }`) as `(output_column, canonical_type)`.
pub fn plugin_columns(pipeline: &Pipeline, registry: &PluginRegistry) -> Vec<(String, Type)> {
    pipeline
        .plugin_transforms
        .iter()
        .filter_map(|call| {
            let meta = registry.metadata(&call.plugin_name).ok()?;
            Some((call.output_column.clone(), meta.canonical_output_type()?))
        })
        .collect()
}
