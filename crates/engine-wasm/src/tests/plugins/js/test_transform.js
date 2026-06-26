// JS counterpart of plugins/test_transform (adds two f64 fields).
// Compiled into the JS runtime WASM via `stratum-plugin-compiler` and used by
// the parity tests to assert it behaves identically to the Rust fixture.
const { transform } = require("@stratum/plugin-sdk");

transform("test_transform", {
  version: "1.0.0",
  output: "f64",
  input: {
    a: "f64",
    b: "f64",
  },
  compute({ a, b }) {
    return a + b;
  },
});
