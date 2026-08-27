use crate::{
    error::WasmError,
    runtime::{
        engine::{WasmEngine, WasmEngineConfig},
        instance::PluginInstance,
        limits::{HostCapabilities, ResourceLimits},
    },
    schema::{PluginMetadata, PluginType},
};
use model::{
    core::types::Type,
    execution::{
        pipeline::{Pipeline, ValidationKind},
        plugin::PluginDecl,
    },
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
pub fn caps_from_decl(
    decl: &PluginDecl,
    resolve_env: &dyn Fn(&str) -> Option<String>,
) -> HostCapabilities {
    let env = decl
        .allow_env
        .iter()
        .filter_map(|name| resolve_env(name).map(|value| (name.clone(), value)))
        .collect();

    HostCapabilities {
        http_client: decl.allow_http,
        http_allowed_hosts: decl.allow_http_hosts.clone(),
        key_value_store: decl.allow_kv,
        logging: decl.allow_log,
        metrics: decl.allow_metrics,
        env,
        fs_read: decl.allow_fs_read.clone(),
        fs_write: decl.allow_fs_write.clone(),
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
pub fn load_registry(
    decls: &[PluginDecl],
    resolve_env: &dyn Fn(&str) -> Option<String>,
) -> Result<Arc<PluginRegistry>, WasmError> {
    let mut registry = PluginRegistry::new(&WasmEngineConfig::default())?;
    for decl in decls {
        registry.load_decl(decl, resolve_env)?;
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
        info!(plugin = %def.name, path = %def.path.display(), "plugin loaded");
        Ok(())
    }

    /// Load a plugin straight from its SMQL declaration. Compiles the module,
    /// reads its metadata, and sizes resource limits from the plugin's runtime
    /// hint (so JS plugins get the fuel QuickJS boot needs without the author
    /// having to spell it out) with explicit SMQL overrides applied on top.
    pub fn load_decl(
        &mut self,
        decl: &PluginDecl,
        resolve_env: &dyn Fn(&str) -> Option<String>,
    ) -> Result<(), WasmError> {
        if self.modules.contains_key(&decl.name) {
            return Ok(());
        }

        let module = self.engine.load_module(&decl.path)?;
        let meta = self.engine.read_metadata(&module)?;
        let def = PluginDef {
            name: decl.name.clone(),
            path: decl.path.clone(),
            capabilities: caps_from_decl(decl, resolve_env),
            limits: resolve_limits(&meta, decl),
            config_json: decl.config_json.clone(),
        };
        self.modules.insert(decl.name.clone(), (module, def));
        info!(plugin = %decl.name, path = %decl.path.display(), "plugin loaded");
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

pub fn unexecutable_plugin_reason(
    pipeline: &Pipeline,
    registry: &PluginRegistry,
) -> Option<String> {
    let check_role = |name: &str, expected: PluginType| -> Option<String> {
        match registry.metadata(name) {
            Ok(m) if m.plugin_type == expected => None,
            Ok(m) => Some(format!(
                "plugin '{name}' is a {:?}, but is used as a {:?}",
                m.plugin_type, expected
            )),
            Err(_) => Some(format!("plugin '{name}' failed to load")),
        }
    };

    pipeline
        .plugin_transforms
        .iter()
        .find_map(|call| check_role(&call.plugin_name, PluginType::Transform))
        .or_else(|| {
            pipeline.validations.iter().find_map(|rule| {
                if let ValidationKind::WasmFilter { plugin_name, .. } = &rule.kind {
                    check_role(plugin_name, PluginType::Filter)
                } else {
                    None
                }
            })
        })
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

#[cfg(test)]
mod tests {
    use super::*;

    fn decl() -> PluginDecl {
        PluginDecl {
            name: "p".into(),
            path: PathBuf::from("p.wasm"),
            allow_http: false,
            allow_http_hosts: Vec::new(),
            allow_kv: false,
            allow_log: true,
            allow_metrics: false,
            allow_fs_read: vec![PathBuf::from("/data/in")],
            allow_fs_write: vec![PathBuf::from("/data/out")],
            allow_env: vec!["API_KEY".into(), "MISSING".into()],
            memory_limit_bytes: None,
            fuel_limit: None,
            timeout_ms: None,
            config_json: None,
        }
    }

    #[test]
    fn caps_from_decl_resolves_env_and_carries_fs() {
        let d = decl();
        // Only API_KEY resolves; MISSING (None) is dropped, not exposed empty.
        let caps = caps_from_decl(&d, &|name| {
            (name == "API_KEY").then(|| "s3cr3t".to_string())
        });
        assert_eq!(caps.env, vec![("API_KEY".into(), "s3cr3t".into())]);
        assert_eq!(caps.fs_read, vec![PathBuf::from("/data/in")]);
        assert_eq!(caps.fs_write, vec![PathBuf::from("/data/out")]);
        assert!(caps.logging);
    }

    #[test]
    fn caps_from_decl_exposes_no_env_when_nothing_resolves() {
        let caps = caps_from_decl(&decl(), &|_| None);
        assert!(
            caps.env.is_empty(),
            "unresolved env names must not be exposed"
        );
    }
}
