// Benchmark transform plugin (JavaScript -> WASM via QuickJS): net = amount * quantity.
//
// Deliberately identical logic to ../rust/order_net so the two plugin runtimes
// (native Rust WASM vs JS-on-QuickJS WASM) are measured on the same per-row
// transform - isolating each runtime's invocation cost, not the arithmetic.
const { transform } = require("@stratum/plugin-sdk");

transform("order_net", {
  version: "1.0.0",
  output: "f64",
  input: { amount: "f64", quantity: "f64" },
  compute({ amount, quantity }) {
    return (amount ?? 0) * (quantity ?? 0);
  },
});
