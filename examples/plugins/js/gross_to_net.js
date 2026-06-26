// Transform - multi-numeric input. Net = gross * (1 - rate). The rate is a
// mapped input COLUMN, not plugin config.
// Test:  plugin test gross_to_net.js --input '{"gross":100,"rate":0.2}'
//        -> Float(80.0)
const { transform } = require("@stratum/plugin-sdk");

transform("gross_to_net", {
  version: "1.0.0",
  output: "f64",
  input: { gross: "f64", rate: "f64" },
  compute({ gross, rate }) {
    return (gross ?? 0) * (1 - (rate ?? 0));
  },
});
