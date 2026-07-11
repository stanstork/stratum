/// Absolute path to a compiled plugin fixture under `engine-wasm`, resolved from
/// this crate's manifest dir so it works regardless of the test CWD.
pub(crate) fn fixture(name: &str) -> String {
    format!(
        "{}/../engine-wasm/src/tests/fixtures/{}",
        env!("CARGO_MANIFEST_DIR"),
        name
    )
}

mod benchmark;
mod combined;
mod filter;
mod memory_limit;
mod resume;
mod sink;
mod source;
mod source_to_sink;
mod transform;
mod verify;
