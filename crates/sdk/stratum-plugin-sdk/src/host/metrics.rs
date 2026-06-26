#[cfg(target_arch = "wasm32")]
mod ffi {
    #[link(wasm_import_module = "stratum")]
    unsafe extern "C" {
        pub fn metric_counter(name_ptr: u32, name_len: u32, value: i64);
        pub fn metric_gauge(name_ptr: u32, name_len: u32, value: f64);
    }
}

#[cfg(not(target_arch = "wasm32"))]
mod ffi {
    pub unsafe fn metric_counter(_np: u32, _nl: u32, _v: i64) {}
    pub unsafe fn metric_gauge(_np: u32, _nl: u32, _v: f64) {}
}

/// Add `value` to the named counter on the host.
pub fn metric_counter(name: &str, value: i64) {
    let b = name.as_bytes();
    unsafe { ffi::metric_counter(b.as_ptr() as u32, b.len() as u32, value) };
}

/// Set the named gauge to `value` on the host.
pub fn metric_gauge(name: &str, value: f64) {
    let b = name.as_bytes();
    unsafe { ffi::metric_gauge(b.as_ptr() as u32, b.len() as u32, value) };
}
