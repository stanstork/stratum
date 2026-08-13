// Transform - single string in, string out. Smallest possible string plugin.
// Test:  plugin test upper.js --input '{"name":"ada"}'   ->  String("ADA")
const { transform } = require("@stratum/plugin-sdk");

transform("upper", {
  version: "1.0.0",
  output: "string",
  input: { name: "string" },
  compute(rows) {
    return rows.map(({ name }) => (name ?? "").toUpperCase());
  },
});
