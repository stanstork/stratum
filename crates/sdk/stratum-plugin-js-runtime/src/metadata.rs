/// Capacity of the bundled-JS placeholder. Keep in sync with the compiler.
pub const USER_JS_CAP: usize = 4 * 1024 * 1024;
/// Capacity of the metadata placeholder. Keep in sync with the compiler.
pub const METADATA_CAP: usize = 64 * 1024;

/// Marker at the start of `USER_JS`. Keep in sync with the compiler.
pub const USER_JS_MAGIC: [u8; 16] = *b"STRATUM_USR_JS01";
/// Marker at the start of `METADATA`. Keep in sync with the compiler.
pub const METADATA_MAGIC: [u8; 16] = *b"STRATUM_META_J01";

/// Build a placeholder buffer: the 16-byte magic followed by non-zero filler so
/// the whole region is emitted as data (not elided as zeros).
const fn placeholder<const N: usize>(magic: [u8; 16]) -> [u8; N] {
    let mut buf = [0xDBu8; N];
    let mut i = 0;
    while i < magic.len() {
        buf[i] = magic[i];
        i += 1;
    }
    buf
}

#[unsafe(no_mangle)]
pub static USER_JS: [u8; USER_JS_CAP] = placeholder(USER_JS_MAGIC);

#[unsafe(no_mangle)]
pub static METADATA: [u8; METADATA_CAP] = placeholder(METADATA_MAGIC);

pub fn user_js_bytes() -> &'static str {
    let end = USER_JS
        .iter()
        .position(|&b| b == 0)
        .unwrap_or(USER_JS.len());
    std::str::from_utf8(&USER_JS[..end]).unwrap_or("")
}

pub fn metadata_bytes() -> &'static [u8] {
    let end = METADATA
        .iter()
        .position(|&b| b == 0)
        .unwrap_or(METADATA.len());
    &METADATA[..end]
}
