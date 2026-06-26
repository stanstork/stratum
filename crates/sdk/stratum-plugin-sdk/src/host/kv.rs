use crate::error::PluginResult;

#[cfg(target_arch = "wasm32")]
mod ffi {
    #[link(wasm_import_module = "stratum")]
    unsafe extern "C" {
        pub fn kv_get(key_ptr: u32, key_len: u32) -> u64;
        pub fn kv_set(key_ptr: u32, key_len: u32, val_ptr: u32, val_len: u32);
    }
}

#[cfg(not(target_arch = "wasm32"))]
mod ffi {
    pub unsafe fn kv_get(_kp: u32, _kl: u32) -> u64 {
        0
    }
    pub unsafe fn kv_set(_kp: u32, _kl: u32, _vp: u32, _vl: u32) {}
}

/// Read a value from the host's key-value store. Returns `Ok(None)` if the key
/// is absent or the capability is not granted.
pub fn kv_get(key: &str) -> PluginResult<Option<Vec<u8>>> {
    let kb = key.as_bytes();
    let packed = unsafe { ffi::kv_get(kb.as_ptr() as u32, kb.len() as u32) };
    if packed == 0 {
        return Ok(None);
    }
    let (ptr, len) = crate::runtime::pack::unpack(packed);
    Ok(Some(unsafe {
        crate::runtime::abi::read_from_guest(ptr, len)
    }))
}

/// Write a value to the host's key-value store. No-op if the capability is
/// not granted.
pub fn kv_set(key: &str, value: &[u8]) -> PluginResult<()> {
    let kb = key.as_bytes();
    unsafe {
        ffi::kv_set(
            kb.as_ptr() as u32,
            kb.len() as u32,
            value.as_ptr() as u32,
            value.len() as u32,
        );
    }
    Ok(())
}
