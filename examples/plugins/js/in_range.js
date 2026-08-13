// Filter - numeric bounds with two mapped columns (value + an upper bound from
// the row). Demonstrates a filter that reads more than one field.
// Test:  plugin test in_range.js --input '{"value":5,"max":10}'   -> PASS
//        plugin test in_range.js --input '{"value":50,"max":10}'  -> REJECT
//        plugin test in_range.js --input '{"value":0,"max":10}'   -> PASS (>=0 boundary)
const { filter } = require("@stratum/plugin-sdk");

filter("in_range", {
  version: "1.0.0",
  input: { value: "i64", max: "i64" },
  evaluate(rows) {
    return rows.map(({ value, max }) =>
      value >= 0 && value <= max
        ? { pass: true }
        : { pass: false, reason: `value ${value} outside [0, ${max}]` }
    );
  },
});
