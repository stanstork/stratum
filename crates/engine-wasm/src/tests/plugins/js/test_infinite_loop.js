// JS counterpart of plugins/test_infinite_loop.
// Burns CPU forever inside QuickJS so the host's fuel limit trips. Verifies the
// runtime kills runaway JS plugins the same way it kills runaway Rust plugins.
const { transform } = require("@stratum/plugin-sdk");

transform("test_infinite_loop", {
  version: "1.0.0",
  output: "f64",
  input: {},
  compute() {
    // eslint-disable-next-line no-constant-condition
    while (true) {
      // spin
    }
  },
});
