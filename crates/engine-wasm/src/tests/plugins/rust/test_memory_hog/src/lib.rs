use stratum_plugin_sdk::{PluginInput, PluginResult, stratum_transform};

/// Grows the WASM linear memory directly until the host's
/// `ResourceLimiter` denies the growth, then traps with `unreachable`.
/// Used by `test_memory_limit` to verify the runtime enforces memory
/// caps.
#[stratum_transform(
    name = "test_memory_hog",
    version = "1.0.0",
    output = "f64",
    input = []
)]
fn hog(_input: PluginInput) -> PluginResult<f64> {
    loop {
        // Grow by 16 pages (1 MB) per iteration. `memory_grow` returns
        // `usize::MAX` when the host refuses.
        let prev = core::arch::wasm32::memory_grow(0, 16);
        if prev == usize::MAX {
            core::arch::wasm32::unreachable();
        }
    }
}
