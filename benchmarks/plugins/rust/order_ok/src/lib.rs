//! Benchmark WASM *filter* plugin (native Rust -> WASM): pass if its
//! `amount` input is non-negative. Deliberately trivial so the benchmark
//! isolates the per-row plugin-filter invocation cost (the WASM boundary + value
//! marshalling), not the predicate. Paired with ../../js/order_ok.js (identical
//! logic in JS) so the two plugin runtimes are compared on the same filter work.
use stratum_plugin_sdk::{stratum_filter, FilterDecision, PluginInput, PluginResult};

#[stratum_filter(
    name = "order_ok",
    version = "1.0.0",
    input = [
        { name = "amount", type = "f64", nullable = false },
    ]
)]
fn order_ok(inputs: Vec<PluginInput>) -> PluginResult<Vec<FilterDecision>> {
    inputs
        .iter()
        .map(|input| {
            let amount = input.get_f64("amount")?;
            Ok(if amount >= 0.0 {
                FilterDecision::pass()
            } else {
                FilterDecision::reject("amount must be non-negative")
            })
        })
        .collect()
}
