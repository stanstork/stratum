// JS counterpart of plugins/test_memory_hog.
// Allocates without bound so the WASM linear memory keeps growing until the
// host's ResourceLimiter denies it and the instance traps. Verifies the runtime
// enforces memory caps on JS plugins.
const { transform } = require("@stratum/plugin-sdk");

transform("test_memory_hog", {
  version: "1.0.0",
  output: "f64",
  input: {},
  compute() {
    const chunks = [];
    // eslint-disable-next-line no-constant-condition
    while (true) {
      // ~1 MB per iteration; QuickJS will request more linear memory until the
      // host refuses the growth.
      chunks.push(new Uint8Array(1024 * 1024));
    }
  },
});
