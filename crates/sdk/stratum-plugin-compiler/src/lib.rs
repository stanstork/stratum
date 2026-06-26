pub use error::CompileError;
use std::borrow::Cow;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::process::Command;

mod error;

/// Size of the `USER_JS` placeholder baked into the runtime WASM.
/// Must match `stratum-plugin-js-runtime/src/metadata.rs`.
const USER_JS_CAP: usize = 4 * 1024 * 1024;
/// Size of the `METADATA` placeholder baked into the runtime WASM.
const METADATA_CAP: usize = 64 * 1024;
/// Magic markers at the start of each placeholder. Must match
/// `stratum-plugin-js-runtime/src/metadata.rs`.
const USER_JS_MAGIC: &[u8; 16] = b"STRATUM_USR_JS01";
const METADATA_MAGIC: &[u8; 16] = b"STRATUM_META_J01";

/// Options for compiling a JavaScript plugin to WASM.
#[derive(Debug, Clone, Default)]
pub struct CompileOpts {
    /// Minify the bundled JS (esbuild `--minify`).
    pub minify: bool,
    /// Emit an inline sourcemap in the bundle.
    pub source_map: bool,
    /// Override the esbuild binary instead of using the one on `PATH`.
    pub esbuild_path: Option<PathBuf>,
    /// Override the runtime WASM blob to patch. Defaults to
    /// `$STRATUM_JS_RUNTIME` or `stratum-plugin-js-runtime.wasm` in the CWD.
    pub runtime_wasm: Option<PathBuf>,
    /// Runtime WASM bytes to patch when no path override / env var is set.
    pub runtime_wasm_bytes: Option<Cow<'static, [u8]>>,
}

/// A successfully compiled plugin: the patched WASM bytes plus the metadata
/// extracted from the plugin's registration.
#[derive(Debug, Clone)]
pub struct CompiledPlugin {
    pub wasm: Vec<u8>,
    pub metadata: serde_json::Value,
    /// Plugin role: `transform` | `filter` | `source` | `sink`.
    pub role: String,
}

/// Compile a JS plugin to WASM in memory, without touching the filesystem for
/// output. Useful for tests and inspection.
pub fn compile(input: &Path, opts: &CompileOpts) -> Result<CompiledPlugin, CompileError> {
    let bundle = bundle_with_esbuild(input, opts)?;
    let metadata = extract_metadata(&bundle)?;

    let role = metadata
        .get("type")
        .and_then(|v| v.as_str())
        .ok_or_else(|| CompileError::Metadata("plugin metadata is missing 'type'".into()))?
        .to_string();
    let metadata_json = serde_json::to_vec(&metadata)
        .map_err(|e| CompileError::Metadata(format!("serializing metadata: {e}")))?;

    let runtime = load_runtime_wasm(opts)?;
    let wasm = patch_runtime(&runtime, bundle.as_bytes(), &metadata_json)?;
    validate_exports(&wasm, &role)?;

    Ok(CompiledPlugin {
        wasm,
        metadata,
        role,
    })
}

/// Compile a JS plugin and write the resulting WASM to `output`.
pub fn compile_to_file(
    input: &Path,
    output: &Path,
    opts: &CompileOpts,
) -> Result<CompiledPlugin, CompileError> {
    let compiled = compile(input, opts)?;
    std::fs::write(output, &compiled.wasm)
        .map_err(|e| CompileError::Io(format!("writing {}: {e}", output.display())))?;
    Ok(compiled)
}

/// Result of [`compile_cached`]: the compiled WASM path and whether it was
/// served from the cache (no recompilation) or freshly built.
#[derive(Debug, Clone)]
pub struct CachedBuild {
    pub path: PathBuf,
    pub from_cache: bool,
}

/// Compile a JS plugin to WASM under `cache_dir`, reusing a previous build when the inputs are unchanged.
pub fn compile_cached(
    input: &Path,
    opts: &CompileOpts,
    cache_dir: &Path,
) -> Result<CachedBuild, CompileError> {
    let source = std::fs::read(input)
        .map_err(|e| CompileError::Io(format!("reading {}: {e}", input.display())))?;
    let runtime = load_runtime_wasm(opts)?;

    let mut hasher = blake3::Hasher::new();
    hasher.update(&source);
    hasher.update(&runtime);
    hasher.update(&[opts.minify as u8, opts.source_map as u8]);
    let key = hasher.finalize().to_hex();
    let stem = input
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("plugin");
    let out = cache_dir.join(format!("{stem}-{}.wasm", &key.as_str()[..16]));

    if out.is_file() {
        return Ok(CachedBuild {
            path: out,
            from_cache: true,
        });
    }

    std::fs::create_dir_all(cache_dir).map_err(|e| {
        CompileError::Io(format!("creating cache dir {}: {e}", cache_dir.display()))
    })?;
    compile_to_file(input, &out, opts)?;
    Ok(CachedBuild {
        path: out,
        from_cache: false,
    })
}

/// How to invoke esbuild: a direct binary, or `npx esbuild` (Node fallback).
enum Bundler {
    Bin(PathBuf),
    /// `npx [--yes] esbuild`, with the resolved path to the `npx` launcher.
    Npx(PathBuf),
}

/// Resolve which esbuild to run. Priority: explicit `opts.esbuild_path`, then
/// `$STRATUM_ESBUILD`, then `esbuild` on `PATH`, then `npx esbuild` when Node is
/// available. Errors when none are found.
fn resolve_esbuild(opts: &CompileOpts) -> Result<Bundler, CompileError> {
    if let Some(p) = &opts.esbuild_path {
        return Ok(Bundler::Bin(p.clone()));
    }
    if let Some(p) = std::env::var_os("STRATUM_ESBUILD") {
        return Ok(Bundler::Bin(PathBuf::from(p)));
    }
    if let Some(p) = find_on_path("esbuild") {
        return Ok(Bundler::Bin(p));
    }
    if let Some(p) = find_on_path("npx") {
        return Ok(Bundler::Npx(p));
    }
    Err(CompileError::Bundle(
        "no esbuild found: install esbuild (or Node.js for `npx`), set $STRATUM_ESBUILD, \
         or pass esbuild_path"
            .into(),
    ))
}

/// Find `name` on `PATH`, returning the resolved path. On Windows the on-disk
/// file usually carries an extension (e.g. `npx` -> `npx.cmd`), so each
/// `PATHEXT` suffix is tried in turn.
fn find_on_path(name: &str) -> Option<PathBuf> {
    let paths = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&paths) {
        for ext in path_exts() {
            let candidate = if ext.is_empty() {
                dir.join(name)
            } else {
                dir.join(format!("{name}{ext}"))
            };
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }
    None
}

/// Executable suffixes to try when resolving a name on `PATH`. The empty string
/// (exact name) is always tried first.
#[cfg(windows)]
fn path_exts() -> Vec<String> {
    let mut exts = vec![String::new()];
    let pathext = std::env::var("PATHEXT").unwrap_or_else(|_| ".COM;.EXE;.BAT;.CMD".to_string());
    exts.extend(
        pathext
            .split(';')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string()),
    );
    exts
}

#[cfg(not(windows))]
fn path_exts() -> Vec<String> {
    vec![String::new()]
}

/// Build a `Command` for `program`. On Windows, `.cmd`/`.bat` shims (such as the
/// `npx`/`esbuild` launchers npm installs) cannot be started by `CreateProcess`
/// directly and must be run through `cmd.exe /C`.
fn command_for(program: &Path) -> Command {
    #[cfg(windows)]
    {
        let is_script = program
            .extension()
            .and_then(|e| e.to_str())
            .map(|e| e.eq_ignore_ascii_case("cmd") || e.eq_ignore_ascii_case("bat"))
            .unwrap_or(false);
        if is_script {
            let mut c = Command::new("cmd");
            c.arg("/C").arg(program);
            return c;
        }
    }
    Command::new(program)
}

/// Bundle the user's JS together with `@stratum/plugin-sdk` into a single
/// self-contained IIFE script, exposing the SDK's metadata/dispatch hooks on
/// `globalThis`. Shells out to esbuild (subprocess for v1).
fn bundle_with_esbuild(input: &Path, opts: &CompileOpts) -> Result<String, CompileError> {
    let (mut cmd, label) = match resolve_esbuild(opts)? {
        Bundler::Bin(p) => {
            let label = p.display().to_string();
            (command_for(&p), label)
        }
        Bundler::Npx(p) => {
            let mut c = command_for(&p);
            c.arg("--yes").arg("esbuild");
            (c, "npx esbuild".to_string())
        }
    };

    cmd.arg(input)
        .arg("--bundle")
        .arg("--format=iife")
        .arg("--platform=neutral")
        // `neutral` ignores package.json `main`/`module` unless we name the
        // resolution fields explicitly, so the SDK's `"main"` would not resolve.
        .arg("--main-fields=module,main")
        .arg("--target=es2020");
    if opts.minify {
        cmd.arg("--minify");
    }
    if opts.source_map {
        cmd.arg("--sourcemap=inline");
    }

    let output = cmd.output().map_err(|e| {
        CompileError::Bundle(format!(
            "could not run esbuild ('{label}'): {e}. Install esbuild or set esbuild_path."
        ))
    })?;

    if !output.status.success() {
        return Err(CompileError::Bundle(format!(
            "esbuild failed:\n{}",
            String::from_utf8_lossy(&output.stderr)
        )));
    }

    // The SDK's registry module attaches __stratum_get_metadata /
    // __stratum_dispatch to globalThis as a load-time side effect, so the
    // bundle is self-exposing - nothing to append here. (A `require(...)` footer
    // would be wrong anyway: QuickJS has no `require`, and esbuild has already
    // inlined the modules.)
    let bundle = String::from_utf8(output.stdout)
        .map_err(|e| CompileError::Bundle(format!("esbuild output not utf-8: {e}")))?;
    Ok(bundle)
}

/// Run the bundle in an in-process QuickJS context and read back the metadata
/// JSON the SDK registered. Only the role-registration functions run here;
/// no plugin handler is invoked, so host capabilities are not needed.
fn extract_metadata(bundle: &str) -> Result<serde_json::Value, CompileError> {
    use rquickjs::{Context, Function, Runtime};

    let runtime =
        Runtime::new().map_err(|e| CompileError::Metadata(format!("quickjs init: {e}")))?;
    let context = Context::full(&runtime)
        .map_err(|e| CompileError::Metadata(format!("quickjs context: {e}")))?;

    let json: String = context.with(|ctx| -> Result<String, CompileError> {
        ctx.eval::<(), _>(bundle)
            .map_err(|e| CompileError::Metadata(format!("evaluating plugin JS: {e}")))?;
        let get_md: Function = ctx.globals().get("__stratum_get_metadata").map_err(|_| {
            CompileError::Metadata(
                "plugin did not register a role (call transform/filter/source/sink)".into(),
            )
        })?;
        get_md
            .call(())
            .map_err(|e| CompileError::Metadata(format!("reading plugin metadata: {e}")))
    })?;

    serde_json::from_str(&json)
        .map_err(|e| CompileError::Metadata(format!("plugin metadata is not valid JSON: {e}")))
}

/// Inject the bundled JS and metadata JSON into the runtime WASM by locating the
/// magic-prefixed `USER_JS` / `METADATA` placeholders in the linear-memory data
/// segments and overwriting them in place.
fn patch_runtime(runtime: &[u8], user_js: &[u8], metadata: &[u8]) -> Result<Vec<u8>, CompileError> {
    let mut module = walrus::Module::from_buffer(runtime)
        .map_err(|e| CompileError::Patch(format!("parsing runtime WASM: {e}")))?;

    patch_placeholder(
        &mut module,
        USER_JS_MAGIC,
        USER_JS_CAP,
        user_js,
        "bundled JS",
    )?;
    patch_placeholder(
        &mut module,
        METADATA_MAGIC,
        METADATA_CAP,
        metadata,
        "metadata",
    )?;

    Ok(module.emit_wasm())
}

/// Find the data segment containing `magic`, then zero the `cap`-byte region
/// starting there and write `payload` (leaving a NUL terminator after it).
fn patch_placeholder(
    module: &mut walrus::Module,
    magic: &[u8; 16],
    cap: usize,
    payload: &[u8],
    label: &str,
) -> Result<(), CompileError> {
    if payload.len() >= cap {
        return Err(CompileError::Patch(format!(
            "{label} is {} bytes; exceeds the {} byte limit",
            payload.len(),
            cap - 1
        )));
    }

    // walrus stores data in an arena, so locate the id + offset first, then
    // mutate. The placeholder starts with the 16-byte magic marker.
    let located = module
        .data
        .iter()
        .find_map(|d| find_subslice(&d.value, magic).map(|off| (d.id(), off)));

    let (id, off) = located.ok_or_else(|| {
        CompileError::Patch(format!(
            "could not find the {label} placeholder marker in the runtime WASM; \
             is the stratum-plugin-js-runtime.wasm blob up to date?"
        ))
    })?;

    let data = module.data.get_mut(id);
    if off + cap > data.value.len() {
        return Err(CompileError::Patch(format!(
            "{label} placeholder is truncated in its data segment ({} of {cap} bytes available)",
            data.value.len() - off
        )));
    }

    let region = &mut data.value[off..off + cap];
    region.iter_mut().for_each(|b| *b = 0);
    region[..payload.len()].copy_from_slice(payload);
    Ok(())
}

/// First index at which `needle` occurs in `haystack`.
fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || haystack.len() < needle.len() {
        return None;
    }
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Parse the patched module and confirm it carries the shared ABI exports plus
/// the role-specific entry point implied by the metadata `type`.
fn validate_exports(wasm: &[u8], role: &str) -> Result<(), CompileError> {
    let module = walrus::Module::from_buffer(wasm)
        .map_err(|e| CompileError::Patch(format!("re-parsing patched WASM: {e}")))?;

    let names: HashSet<&str> = module.exports.iter().map(|e| e.name.as_str()).collect();

    let role_entry = match role {
        "transform" => "__stratum_transform",
        "filter" => "__stratum_evaluate",
        "source" => "__stratum_read_page",
        "sink" => "__stratum_write_batch",
        other => {
            return Err(CompileError::Metadata(format!(
                "unknown plugin type '{other}'"
            )));
        }
    };

    let required = [
        "__stratum_alloc",
        "__stratum_dealloc",
        "__stratum_metadata",
        "__stratum_initialize",
        "__stratum_shutdown",
        role_entry,
    ];
    let missing: Vec<&str> = required
        .iter()
        .copied()
        .filter(|e| !names.contains(e))
        .collect();

    if !missing.is_empty() {
        return Err(CompileError::Patch(format!(
            "patched module is missing required exports: {}",
            missing.join(", ")
        )));
    }
    Ok(())
}

/// Locate the pre-built runtime WASM.
fn load_runtime_wasm(opts: &CompileOpts) -> Result<Vec<u8>, CompileError> {
    // Explicit path override, then env var.
    if let Some(path) = opts
        .runtime_wasm
        .clone()
        .or_else(|| std::env::var_os("STRATUM_JS_RUNTIME").map(PathBuf::from))
    {
        return std::fs::read(&path).map_err(|e| {
            CompileError::Io(format!(
                "could not read runtime WASM at {}: {e}",
                path.display()
            ))
        });
    }

    // Host-embedded runtime (e.g. CLI baked it in with `include_bytes!`).
    if let Some(bytes) = &opts.runtime_wasm_bytes
        && !bytes.is_empty()
    {
        return Ok(bytes.to_vec());
    }

    // Last resort: a copy sitting in the current directory.
    let cwd = PathBuf::from("stratum-plugin-js-runtime.wasm");
    std::fs::read(&cwd).map_err(|e| {
        CompileError::Io(format!(
            "could not read runtime WASM at {}: {e}. \
             Build it with `build_fixtures.sh js` (--runtime-wasm) or set STRATUM_JS_RUNTIME.",
            cwd.display()
        ))
    })
}
