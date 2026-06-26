use std::alloc::{Layout, alloc as rust_alloc, dealloc as rust_dealloc};

/// Allocate `size` bytes in the guest heap and return a pointer.
/// Used by the host to write input buffers before calling a plugin function.
///
/// # Safety
///
/// The returned pointer is valid for `size` bytes. Caller must free via
/// `__stratum_dealloc` using the same size.
#[doc(hidden)]
pub unsafe fn alloc_bytes(size: u32) -> u32 {
    if size == 0 {
        return 0;
    }
    let layout = Layout::from_size_align(size as usize, 1).unwrap();
    unsafe { rust_alloc(layout) as u32 }
}

#[doc(hidden)]
pub unsafe fn dealloc_bytes(ptr: u32, size: u32) {
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
    let ptr = unsafe { alloc_bytes(len) };
    unsafe { std::ptr::copy_nonoverlapping(data.as_ptr(), ptr as *mut u8, data.len()) };
    (ptr, len)
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
