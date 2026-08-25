// Transform - enriches each row from a lookup table on disk. Uses the FS
// capability (`allow_fs_read`) to read the file and the ENV capability
// (`allow_env`) to locate it.
//
// The lookup path comes from the STRATUM_WEIGHTS env var when set, otherwise a
// default. The file is read once and cached for the instance. Returns the weight
// for the row's key, or 0 when the key is absent.
//
// Demonstrates: env.get (read a granted variable), fs.readText (read a file
// inside a granted directory). Both see exactly the sandbox the declaration
// grants - identical to what a Rust plugin gets via std::env / std::fs.
//
// Test: run via configs/file_lookup.smql.
const { transform, fs, env } = require("@stratum/plugin-sdk");

const DEFAULT_PATH = "examples/plugins/data/weights.json";

let weights = null;
function loadWeights() {
  if (weights) return weights;
  const path = env.get("STRATUM_WEIGHTS") || DEFAULT_PATH;
  const text = fs.readText(path);
  weights = text ? JSON.parse(text) : {};
  return weights;
}

transform("weighted_score", {
  version: "1.0.0",
  output: "i64",
  input: { key: "i64" },
  compute(rows) {
    const table = loadWeights();
    return rows.map(({ key }) => Number(table[String(key)] ?? 0));
  },
});
