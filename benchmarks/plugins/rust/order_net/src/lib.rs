//! Benchmark transform plugin (native Rust -> WASM): `net = amount * quantity`.
//!
//! Deliberately identical logic to ../../js/order_net.js so the two plugin
//! runtimes (native Rust compiled to WASM vs JavaScript on QuickJS-in-WASM) are
//! measured on the exact same transform. The transform is trivial on purpose: it
//! isolates each runtime's plugin-invocation cost (the WASM boundary + value
//! marshalling, now amortized across the whole batch) rather than the arithmetic.
use paganel_plugin_sdk::{paganel_transform, PluginInput, PluginResult};

#[paganel_transform(
    name = "order_net",
    version = "1.0.0",
    output = "f64",
    input = [
        { name = "amount", type = "f64", nullable = false },
        { name = "quantity", type = "f64", nullable = false },
    ]
)]
fn order_net(inputs: Vec<PluginInput>) -> PluginResult<Vec<f64>> {
    inputs
        .iter()
        .map(|input| {
            let amount = input.get_f64("amount")?;
            let quantity = input.get_f64("quantity")?;
            Ok(amount * quantity)
        })
        .collect()
}
