// Transform - multi-field input, string output. Exercises a mapping with more
// than one source column.
// Test:  plugin test full_name.js --input '{"first":"Ada","last":"Lovelace"}'
//        -> String("Ada Lovelace")
//
// Note on nullable inputs: `plugin validate` currently requires EVERY declared
// input field to appear in the pipeline mapping, even nullable ones. So if you
// add `middle: { type: "string", nullable: true }` here, every config using
// full_name must still map `middle`. Kept to two required fields to stay clean.
const { transform } = require("@stratum/plugin-sdk");

transform("full_name", {
  version: "1.0.0",
  output: "string",
  input: {
    first: "string",
    last: "string",
  },
  compute({ first, last }) {
    return [first, last].filter(Boolean).join(" ");
  },
});
