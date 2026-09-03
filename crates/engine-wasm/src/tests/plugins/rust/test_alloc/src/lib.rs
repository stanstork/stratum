//! A transform plugin that allocates a configurable amount of memory per row,
//! used to exercise the runtime's memory-limit enforcement.

use std::hint::black_box;
use paganel_plugin_sdk::{PluginInput, PluginResult, config, paganel_transform};

#[paganel_transform(
    name = "test_alloc",
    version = "1.0.0",
    output = "i64",
    input = []
)]
fn alloc(inputs: Vec<PluginInput>) -> PluginResult<Vec<i64>> {
    let mb: usize = config()
        .get("alloc_mb")
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
    let bytes = mb * 1024 * 1024;

    // Write a non-constant value per page so neither the buffer nor the loop can
    // be optimized out. `black_box` keeps the allocation observable.
    let mut buf = vec![0u8; bytes];
    let mut i = 0;
    while i < buf.len() {
        buf[i] = (i as u8).wrapping_add(1);
        i += 4096;
    }
    let mut acc: u64 = 0;
    let mut j = 0;
    while j < buf.len() {
        acc = acc.wrapping_add(buf[j] as u64);
        j += 4096;
    }
    black_box(&buf);
    Ok(vec![black_box(acc) as i64; inputs.len()])
}
