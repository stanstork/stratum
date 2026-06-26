/// Raw host imports. Only available when compiling for wasm32.
#[cfg(target_arch = "wasm32")]
mod ffi {
    #[link(wasm_import_module = "stratum")]
    unsafe extern "C" {
        pub fn log_debug(ptr: u32, len: u32);
        pub fn log_info(ptr: u32, len: u32);
        pub fn log_warn(ptr: u32, len: u32);
        pub fn log_error(ptr: u32, len: u32);
    }
}

// Host-side stubs so the crate compiles on non-wasm targets (tests, host builds).
#[cfg(not(target_arch = "wasm32"))]
mod ffi {
    pub unsafe fn log_debug(_ptr: u32, _len: u32) {}
    pub unsafe fn log_info(_ptr: u32, _len: u32) {}
    pub unsafe fn log_warn(_ptr: u32, _len: u32) {}
    pub unsafe fn log_error(_ptr: u32, _len: u32) {}
}

macro_rules! define_log_fn {
    ($vis:vis $name:ident, $ffi:path) => {
        $vis fn $name(msg: &str) {
            let bytes = msg.as_bytes();
            unsafe { $ffi(bytes.as_ptr() as u32, bytes.len() as u32); }
        }
    };
}

define_log_fn!(pub log_debug, ffi::log_debug);
define_log_fn!(pub log_info,  ffi::log_info);
define_log_fn!(pub log_warn,  ffi::log_warn);
define_log_fn!(pub log_error, ffi::log_error);
