// Benchmark WASM filter plugin (JavaScript -> WASM via QuickJS): pass if
// its `amount` is non-negative. Deliberately identical logic to ../rust/order_ok
// so the two plugin runtimes are measured on the same per-row filter work -
// isolating each runtime's invocation cost, not the predicate.
const { filter } = require("@stratum/plugin-sdk");

filter("order_ok", {
  version: "1.0.0",
  input: { amount: "f64" },
  evaluate(rows) {
    return rows.map(({ amount }) =>
      (amount ?? 0) >= 0 ? { pass: true } : { pass: false, reason: "amount must be non-negative" }
    );
  },
});
