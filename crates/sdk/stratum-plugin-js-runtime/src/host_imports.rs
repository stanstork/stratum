use rquickjs::{Ctx, Function};

#[link(wasm_import_module = "stratum")]
unsafe extern "C" {
    fn log_debug(ptr: u32, len: u32);
    fn log_info(ptr: u32, len: u32);
    fn log_warn(ptr: u32, len: u32);
    fn log_error(ptr: u32, len: u32);
    // (method, url_ptr, url_len, body_ptr, body_len) -> packed (ptr, len) of
    // the response body; 0 means capability denied / error.
    fn http_request(method: u32, url_ptr: u32, url_len: u32, body_ptr: u32, body_len: u32) -> u64;
}

pub fn install<'js>(ctx: &Ctx<'js>) -> Result<(), String> {
    let global = ctx.globals();

    // __host_http_request(method, url, headers, body) -> response JSON string
    // shaped as {status, headers, body} for src/http.js. Headers are not part
    // of the stratum http ABI yet, so they are dropped on the way to the host.
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
            let bytes = unsafe { crate::bootstrap::read_from_guest(ptr, len) };
            let body_text = String::from_utf8(bytes).unwrap_or_default();
            serde_json::json!({ "status": 200, "headers": {}, "body": body_text }).to_string()
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
