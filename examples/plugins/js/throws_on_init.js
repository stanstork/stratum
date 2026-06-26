// Negative fixture - throws at module top level. The compiler's metadata
// extractor EVALUATES the bundle, so the throw aborts there: every command that
// touches this `.js` (`compile`, `inspect`, `validate`, `plan`, `apply`) should
// surface a clean compile error - never a panic or a silent success.
//   plugin compile throws_on_init.js -o /tmp/x.wasm   -> error "boom: ..."
//   plugin inspect throws_on_init.js                  -> same error
const { transform } = require("@stratum/plugin-sdk");

throw new Error("boom: this plugin fails to load on purpose");

// Unreachable - kept so the file still reads as a plugin.
// eslint-disable-next-line no-unreachable
transform("throws_on_init", {
  version: "1.0.0",
  output: "i64",
  input: {},
  compute() {
    return 0;
  },
});
