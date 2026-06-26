use rquickjs::{Context, Function, Persistent, Runtime};
use std::alloc::{Layout, alloc as rust_alloc, dealloc as rust_dealloc};

pub struct JsRuntime {
    /// Owns the QuickJS runtime. Held only to keep it alive for as long as
    /// `context` is used; the context borrows from it internally.
    #[allow(dead_code)]
    pub runtime: Runtime,
    pub context: Context,
    pub dispatch_fn: Persistent<Function<'static>>,
    /// Metadata JSON fetched from the SDK at init.
    #[allow(dead_code)]
    pub metadata_json: String,
    /// Plugin config blob; forwarded to source/sink handlers once lifecycle
    /// dispatch is wired up.
    #[allow(dead_code)]
    pub config_json: Vec<u8>,
}

pub fn initialize(cfg_ptr: u32, cfg_len: u32) -> Result<JsRuntime, String> {
    let config_json = unsafe { read_from_guest(cfg_ptr, cfg_len) };
    let runtime = Runtime::new().map_err(|e| e.to_string())?;
    let context = Context::full(&runtime).map_err(|e| e.to_string())?;

    context.with(|ctx| -> Result<(), String> {
        crate::host_imports::install(&ctx)?;
        let user_src = crate::metadata::user_js_bytes();
        ctx.eval::<(), _>(user_src)
            .map_err(|e| format!("eval: {e}"))?;
        Ok(())
    })?;

    // After eval, the SDK has populated globalThis with __stratum_dispatch
    // and __stratum_get_metadata. Grab persistent handles, and hand the plugin
    // its config (the SMQL `config { }` block) via __stratum_set_config.
    let (dispatch_fn, metadata_json) = context.with(|ctx| -> Result<_, String> {
        let global = ctx.globals();

        if let Ok(set_cfg) = global.get::<_, Function>("__stratum_set_config") {
            let cfg_str = String::from_utf8_lossy(&config_json);
            set_cfg
                .call::<_, ()>((cfg_str.as_ref(),))
                .map_err(|e| format!("set_config: {e}"))?;
        }

        let get_md: Function = global
            .get("__stratum_get_metadata")
            .map_err(|e| e.to_string())?;
        let md: String = get_md.call(()).map_err(|e| e.to_string())?;
        let dispatch: Function = global
            .get("__stratum_dispatch")
            .map_err(|e| e.to_string())?;
        Ok((Persistent::save(&ctx, dispatch), md))
    })?;

    Ok(JsRuntime {
        runtime,
        context,
        dispatch_fn,
        metadata_json,
        config_json,
    })
}

/// Allocate `size` bytes in the guest heap and return a pointer.
/// Used by the host to write input buffers before calling a plugin function.
///
/// # Safety
///
/// The returned pointer is valid for `size` bytes. Caller must free via
/// `__stratum_dealloc` using the same size.
#[doc(hidden)]
pub unsafe fn alloc(size: u32) -> u32 {
    if size == 0 {
        return 0;
    }
    let layout = Layout::from_size_align(size as usize, 1).unwrap();
    unsafe { rust_alloc(layout) as u32 }
}

#[doc(hidden)]
pub unsafe fn dealloc(ptr: u32, size: u32) {
    if ptr == 0 || size == 0 {
        return;
    }
    let layout = Layout::from_size_align(size as usize, 1).unwrap();
    unsafe { rust_dealloc(ptr as *mut u8, layout) };
}

/// Copy `data` into freshly allocated guest memory and return `(ptr, len)`.
#[doc(hidden)]
pub unsafe fn write_to_guest(data: &[u8]) -> (u32, u32) {
    let len = data.len() as u32;
    if len == 0 {
        return (0, 0);
    }
    let ptr = unsafe { alloc(len) };
    unsafe { std::ptr::copy_nonoverlapping(data.as_ptr(), ptr as *mut u8, data.len()) };
    (ptr, len)
}

/// Serialize a runtime-level error into the host's error envelope and return
/// it as a packed `(ptr, len)`. Mirrors the shape produced by the Rust SDK's
/// `serialize_error`: `{"error": msg, "code": "internal", "transient": false}`.
pub fn error_envelope(msg: &str) -> u64 {
    let json = serde_json::to_vec(&serde_json::json!({
        "error": msg,
        "code": "internal",
        "transient": false,
    }))
    .unwrap_or_else(|_| b"{\"error\":\"error serialization failed\"}".to_vec());
    let (p, l) = unsafe { write_to_guest(&json) };
    crate::abi::pack(p, l)
}

/// Read exactly `len` bytes starting at `ptr` into a `Vec<u8>`.
///
/// # Safety
///
/// `ptr..ptr+len` must be valid guest memory.
#[doc(hidden)]
pub unsafe fn read_from_guest(ptr: u32, len: u32) -> Vec<u8> {
    if ptr == 0 || len == 0 {
        return Vec::new();
    }
    unsafe { std::slice::from_raw_parts(ptr as *const u8, len as usize).to_vec() }
}
