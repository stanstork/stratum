use crate::error::CliError;
use engine_core::plan::execution::ExecutionPlan;
use std::borrow::Cow;
use std::path::{Path, PathBuf};
use stratum_plugin_compiler::CompileOpts;
use tracing::{debug, info};

/// JS plugin runtime WASM baked in at build time (see `build.rs`). Empty when
/// no runtime was available during the build, in which case the compiler falls
/// back to `$STRATUM_JS_RUNTIME` / the current directory.
static EMBEDDED_JS_RUNTIME: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/js_runtime.wasm"));

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

/// Compile `src` to WASM via the plugin compiler, caching under
/// `~/.stratum/plugin-cache`. The compiler owns the cache-key/esbuild-discovery
/// logic; here we just supply the embedded runtime and the cache location.
fn compile_cached(src: &Path) -> Result<PathBuf, CliError> {
    let opts = CompileOpts {
        runtime_wasm_bytes: Some(Cow::Borrowed(EMBEDDED_JS_RUNTIME)),
        ..Default::default()
    };
    let dir = cache_dir()?;
    let build = stratum_plugin_compiler::compile_cached(src, &opts, &dir).map_err(|e| {
        CliError::UserMessage(format!("compiling JS plugin {}: {e}", src.display()))
    })?;
    if build.from_cache {
        debug!(plugin = %src.display(), wasm = %build.path.display(), "using cached JS plugin build");
    } else {
        info!(plugin = %src.display(), wasm = %build.path.display(), "compiled JS plugin");
    }
    Ok(build.path)
}

/// `~/.stratum/plugin-cache` - compiled JS plugins keyed by content hash.
fn cache_dir() -> Result<PathBuf, CliError> {
    let home = dirs::home_dir()
        .ok_or_else(|| CliError::UserMessage("cannot locate home dir for plugin cache".into()))?;
    Ok(home.join(".stratum").join("plugin-cache"))
}
