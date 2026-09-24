use rquickjs::{Ctx, Function};

#[link(wasm_import_module = "paganel")]
unsafe extern "C" {
    fn log_debug(ptr: u32, len: u32);
    fn log_info(ptr: u32, len: u32);
    fn log_warn(ptr: u32, len: u32);
    fn log_error(ptr: u32, len: u32);
    fn http_request(method: u32, url_ptr: u32, url_len: u32, body_ptr: u32, body_len: u32) -> u64;
    fn kv_get(key_ptr: u32, key_len: u32) -> u64;
    fn kv_set(key_ptr: u32, key_len: u32, val_ptr: u32, val_len: u32);
    fn metric_counter(name_ptr: u32, name_len: u32, value: i64);
    fn metric_gauge(name_ptr: u32, name_len: u32, value: f64);
}

pub fn install<'js>(ctx: &Ctx<'js>) -> Result<(), String> {
    let global = ctx.globals();

    // __host_http_request(method, url, headers, body) -> response JSON string
    // shaped as {status, headers, body} for src/http.js. Headers are not part
    // of the paganel http ABI yet, so they are dropped on the way to the host.
    let http_req = Function::new(
        ctx.clone(),
        |method: String, url: String, _headers: String, body: String| -> String {
            let u = url.into_bytes();
            let b = body.into_bytes();

            let (up, ul) = unsafe { crate::bootstrap::write_to_guest(&u) };
            let (bp, bl) = unsafe { crate::bootstrap::write_to_guest(&b) };

            let packed = unsafe { http_request(method_code(&method), up, ul, bp, bl) };

            if packed == 0 {
                return r#"{"status":0,"headers":{},"body":""}"#.to_string();
            }

            let ptr = (packed >> 32) as u32;
            let len = (packed & 0xFFFF_FFFF) as u32;
            let raw = unsafe { crate::bootstrap::read_from_guest(ptr, len) };

            // Response frame: [status: u16 LE][body...].
            if raw.len() < 2 {
                return r#"{"status":0,"headers":{},"body":""}"#.to_string();
            }

            let status = u16::from_le_bytes([raw[0], raw[1]]);
            let body_text = String::from_utf8(raw[2..].to_vec()).unwrap_or_default();
            serde_json::json!({ "status": status, "headers": {}, "body": body_text }).to_string()
        },
    )
    .map_err(|e| e.to_string())?;
    global
        .set("__host_http_request", http_req)
        .map_err(|e| e.to_string())?;

    // __host_log(level, msg): level is "error" | "warn" | "info" | "debug".
    let log = Function::new(ctx.clone(), |level: String, msg: String| {
        let m = msg.into_bytes();
        let (ptr, len) = unsafe { crate::bootstrap::write_to_guest(&m) };
        unsafe {
            match level.as_str() {
                "error" => log_error(ptr, len),
                "warn" => log_warn(ptr, len),
                "debug" => log_debug(ptr, len),
                _ => log_info(ptr, len),
            }
        }
    })
    .map_err(|e| e.to_string())?;
    global.set("__host_log", log).map_err(|e| e.to_string())?;

    // __host_kv_get(key) -> string | null. Null = absent or capability denied.
    let kv_get_fn = Function::new(ctx.clone(), |key: String| -> Option<String> {
        let k = key.into_bytes();
        let (kp, kl) = unsafe { crate::bootstrap::write_to_guest(&k) };

        let packed = unsafe { kv_get(kp, kl) };

        if packed == 0 {
            return None;
        }

        let ptr = (packed >> 32) as u32;
        let len = (packed & 0xFFFF_FFFF) as u32;
        let bytes = unsafe { crate::bootstrap::read_from_guest(ptr, len) };
        Some(String::from_utf8_lossy(&bytes).into_owned())
    })
    .map_err(|e| e.to_string())?;
    global
        .set("__host_kv_get", kv_get_fn)
        .map_err(|e| e.to_string())?;

    // __host_kv_set(key, value). No-op if the capability is denied.
    let kv_set_fn = Function::new(ctx.clone(), |key: String, value: String| {
        let k = key.into_bytes();
        let v = value.into_bytes();

        let (kp, kl) = unsafe { crate::bootstrap::write_to_guest(&k) };
        let (vp, vl) = unsafe { crate::bootstrap::write_to_guest(&v) };

        unsafe { kv_set(kp, kl, vp, vl) };
    })
    .map_err(|e| e.to_string())?;
    global
        .set("__host_kv_set", kv_set_fn)
        .map_err(|e| e.to_string())?;

    // __host_metric_counter(name, value): JS numbers are f64; a counter delta is
    // an integer, so it is truncated toward zero to i64.
    let metric_counter_fn = Function::new(ctx.clone(), |name: String, value: f64| {
        let n = name.into_bytes();
        let (np, nl) = unsafe { crate::bootstrap::write_to_guest(&n) };
        unsafe { metric_counter(np, nl, value as i64) };
    })
    .map_err(|e| e.to_string())?;
    global
        .set("__host_metric_counter", metric_counter_fn)
        .map_err(|e| e.to_string())?;

    // __host_metric_gauge(name, value).
    let metric_gauge_fn = Function::new(ctx.clone(), |name: String, value: f64| {
        let n = name.into_bytes();
        let (np, nl) = unsafe { crate::bootstrap::write_to_guest(&n) };
        unsafe { metric_gauge(np, nl, value) };
    })
    .map_err(|e| e.to_string())?;
    global
        .set("__host_metric_gauge", metric_gauge_fn)
        .map_err(|e| e.to_string())?;

    // __host_env(name) -> string | null. Reads the guest's WASI environment,
    // which the host populated with exactly the plugin's `allow_env` grants -
    // so JS sees the same variables a Rust plugin would via std::env.
    let env_get_fn = Function::new(ctx.clone(), |name: String| -> Option<String> {
        std::env::var(&name).ok()
    })
    .map_err(|e| e.to_string())?;
    global
        .set("__host_env", env_get_fn)
        .map_err(|e| e.to_string())?;

    // __host_fs_read(path) -> string | null. Uses the guest's WASI filesystem,
    // limited to the host's `allow_fs_read` preopens - identical sandbox to a
    // Rust plugin's std::fs.
    let fs_read_fn = Function::new(ctx.clone(), |path: String| -> Option<String> {
        std::fs::read_to_string(&path).ok()
    })
    .map_err(|e| e.to_string())?;
    global
        .set("__host_fs_read", fs_read_fn)
        .map_err(|e| e.to_string())?;

    // __host_fs_write(path, contents) -> bool (true on success). Limited to
    // `allow_fs_write` preopens.
    let fs_write_fn = Function::new(ctx.clone(), |path: String, contents: String| -> bool {
        std::fs::write(&path, contents.as_bytes()).is_ok()
    })
    .map_err(|e| e.to_string())?;
    global
        .set("__host_fs_write", fs_write_fn)
        .map_err(|e| e.to_string())?;

    Ok(())
}

/// Map an HTTP method name to the host ABI's method code (matches the Rust
/// SDK's `HttpMethod`).
fn method_code(s: &str) -> u32 {
    match s.to_ascii_uppercase().as_str() {
        "GET" => 0,
        "POST" => 1,
        "PUT" => 2,
        "DELETE" => 3,
        "PATCH" => 4,
        _ => 0,
    }
}
