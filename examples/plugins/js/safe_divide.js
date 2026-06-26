// Transform - demonstrates error handling at the row level. Dividing by zero
// throws; the host surfaces it as a per-row plugin error (it does NOT crash the
// instance). Pair with `on_fail`/DLQ behavior in a pipeline.
// Test:  plugin test safe_divide.js --input '{"num":10,"den":2}'  -> Float(5.0)
//        plugin test safe_divide.js --input '{"num":10,"den":0}'  -> error surfaced
const { transform } = require("@stratum/plugin-sdk");

transform("safe_divide", {
  version: "1.0.0",
  output: "f64",
  input: { num: "f64", den: "f64" },
  compute({ num, den }) {
    if (den === 0) {
      throw new Error("division by zero");
    }
    return num / den;
  },
});
