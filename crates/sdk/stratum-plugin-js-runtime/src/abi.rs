use crate::{bootstrap, dispatch, metadata};
use std::sync::OnceLock;

pub(crate) struct RuntimeCell(pub bootstrap::JsRuntime);

// SAFETY: wasm32 single-threaded execution; no concurrent access ever occurs.
unsafe impl Send for RuntimeCell {}
unsafe impl Sync for RuntimeCell {}

pub(crate) static RUNTIME: OnceLock<RuntimeCell> = OnceLock::new();

#[unsafe(no_mangle)]
pub static __STRATUM_PLUGIN_SENTINEL: u8 = 0;

#[unsafe(no_mangle)]
pub extern "C" fn __stratum_alloc(size: u32) -> u32 {
    unsafe { bootstrap::alloc(size) }
}

#[unsafe(no_mangle)]
pub extern "C" fn __stratum_dealloc(ptr: u32, size: u32) {
    unsafe { bootstrap::dealloc(ptr, size) }
}

#[unsafe(no_mangle)]
pub extern "C" fn __stratum_metadata() -> u64 {
    let bytes = metadata::metadata_bytes();
    let (p, l) = unsafe { bootstrap::write_to_guest(bytes) };
    pack(p, l)
}

#[unsafe(no_mangle)]
pub extern "C" fn __stratum_initialize(ptr: u32, len: u32) -> u32 {
    match bootstrap::initialize(ptr, len) {
        Ok(rt) => {
            let _ = RUNTIME.set(RuntimeCell(rt));
            0
        }
        Err(_) => 1,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn __stratum_shutdown() {}

#[unsafe(no_mangle)]
pub extern "C" fn __stratum_transform(ptr: u32, len: u32) -> u64 {
    dispatch::call_role("transform", ptr, len)
}

#[unsafe(no_mangle)]
pub extern "C" fn __stratum_evaluate(ptr: u32, len: u32) -> u64 {
    dispatch::call_role("filter", ptr, len)
}

#[unsafe(no_mangle)]
pub extern "C" fn __stratum_read_page(ptr: u32, len: u32) -> u64 {
    dispatch::call_role("source", ptr, len)
}

#[unsafe(no_mangle)]
pub extern "C" fn __stratum_write_batch(ptr: u32, len: u32) -> u64 {
    dispatch::call_role("sink", ptr, len)
}

#[unsafe(no_mangle)]
pub extern "C" fn __stratum_estimated_count() -> i64 {
    -1
}

#[unsafe(no_mangle)]
pub extern "C" fn __stratum_prepare(ptr: u32, len: u32) -> u32 {
    dispatch::call_lifecycle("prepare", ptr, len)
}

#[unsafe(no_mangle)]
pub extern "C" fn __stratum_finalize() -> u32 {
    dispatch::call_lifecycle("finalize", 0, 0)
}

pub(crate) fn pack(ptr: u32, len: u32) -> u64 {
    ((ptr as u64) << 32) | (len as u64)
}
