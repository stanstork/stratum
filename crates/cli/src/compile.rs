use crate::error::CliError;
use engine_core::plan::execution::ExecutionPlan;
use paganel_plugin_compiler::CompileOpts;
use std::path::{Path, PathBuf};
use tracing::{debug, info};

/// Rewrite every plugin whose `path` is a `.js` source into a compiled `.wasm`,
/// compiling on demand. No-op for plugins that already point at WASM.
pub fn ensure_plugins_compiled(plan: &mut ExecutionPlan) -> Result<(), CliError> {
    for decl in &mut plan.plugins {
        if decl.path.extension().and_then(|e| e.to_str()) == Some("js") {
            decl.path = compile_cached(&decl.path)?;
        }
    }
    Ok(())
}

/// Resolve a single plugin path for direct loading (`plugin inspect|test`): a
/// `.js` source is compiled (cached) and the WASM path returned; any other path
/// is returned unchanged.
pub fn resolve_plugin_wasm(path: &Path) -> Result<PathBuf, CliError> {
    if path.extension().and_then(|e| e.to_str()) == Some("js") {
        compile_cached(path)
    } else {
        Ok(path.to_path_buf())
    }
}

/// Compile `src` to WASM via the plugin compiler, caching under `~/.paganel/plugin-cache`.
fn compile_cached(src: &Path) -> Result<PathBuf, CliError> {
    let opts = CompileOpts::default();
    let dir = cache_dir()?;
    let build = paganel_plugin_compiler::compile_cached(src, &opts, &dir).map_err(|e| {
        CliError::UserMessage(format!("compiling JS plugin {}: {e}", src.display()))
    })?;
    if build.from_cache {
        debug!(plugin = %src.display(), wasm = %build.path.display(), "using cached JS plugin build");
    } else {
        info!(plugin = %src.display(), wasm = %build.path.display(), "compiled JS plugin");
    }
    Ok(build.path)
}

/// `~/.paganel/plugin-cache` - compiled JS plugins keyed by content hash.
fn cache_dir() -> Result<PathBuf, CliError> {
    let home = dirs::home_dir()
        .ok_or_else(|| CliError::UserMessage("cannot locate home dir for plugin cache".into()))?;
    Ok(home.join(".paganel").join("plugin-cache"))
}
