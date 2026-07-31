//! Benchmark transform plugin (native Rust -> WASM): `net = amount * quantity`.
//!
//! Deliberately identical logic to ../../js/order_net.js so the two plugin
//! runtimes (native Rust compiled to WASM vs JavaScript on QuickJS-in-WASM) are
//! measured on the exact same per-row transform. The transform is trivial on
//! purpose: it isolates the per-row plugin-invocation cost (the WASM boundary +
//! value marshalling) of each runtime rather than the arithmetic.
use stratum_plugin_sdk::{PluginInput, PluginResult, stratum_transform};

#[stratum_transform(
    name = "order_net",
    version = "1.0.0",
    output = "f64",
    input = [
        { name = "amount", type = "f64", nullable = false },
        { name = "quantity", type = "f64", nullable = false },
    ]
)]
fn order_net(input: PluginInput) -> PluginResult<f64> {
    let amount = input.get_f64("amount")?;
    let quantity = input.get_f64("quantity")?;
    Ok(amount * quantity)
}
