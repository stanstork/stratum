// Transform - reads plugin CONFIG (not a row column). Config reaches a JS
// transform/filter as the SECOND handler arg. Net = price * (1 - rate), where
// rate comes from `config { rate = "0.2" }` (or `--config-json`).
// Test:  plugin test discount.js --config-json <(echo '{"rate":"0.2"}') \
//          --input '{"price":100}'                  -> Float(80.0)
//        plugin test discount.js --input '{"price":100}'   -> Float(100.0) (no config)
const { transform } = require("@stratum/plugin-sdk");

transform("discount", {
  version: "1.0.0",
  output: "f64",
  input: { price: "f64" },
  compute({ price }, config) {
    const rate = parseFloat(config.rate ?? "0");
    return (price ?? 0) * (1 - (Number.isNaN(rate) ? 0 : rate));
  },
});
