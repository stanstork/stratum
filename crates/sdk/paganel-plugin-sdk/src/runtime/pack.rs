/// Pack a (ptr, len) pair into a single u64.
/// High 32 bits = pointer, low 32 bits = length.
#[inline]
pub fn pack(ptr: u32, len: u32) -> u64 {
    ((ptr as u64) << 32) | (len as u64)
}

/// Unpack a u64 into (ptr, len).
#[inline]
pub fn unpack(packed: u64) -> (u32, u32) {
    ((packed >> 32) as u32, (packed & 0xFFFF_FFFF) as u32)
}
