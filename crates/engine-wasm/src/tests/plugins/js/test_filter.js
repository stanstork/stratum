// JS counterpart of plugins/test_filter (passes only positive values).
const { filter } = require("@stratum/plugin-sdk");

filter("test_filter", {
  version: "1.0.0",
  input: {
    value: "i64",
  },
  evaluate({ value }) {
    if (value > 0) {
      return { pass: true };
    }
    return { pass: false, reason: "value must be positive" };
  },
});
