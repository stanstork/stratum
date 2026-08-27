// Transform - a running total across rows, using the KV capability (`allow_kv`)
// for state and the METRICS capability (`allow_metrics`) for observability.
//
// KV is instance-scoped: the total accumulates across every batch this plugin
// instance processes during the run, but is NOT persisted - the next run starts
// from zero. (With parallel `lanes`, each lane is its own instance, so the total
// would be per-lane.)
//
// Demonstrates: kv.get / kv.set (carry state between rows), metrics.counter /
// metrics.gauge (emit on the host's `plugin::metrics` tracing target).
//
// Test: run via configs/stateful_kv.smql.
const { transform, kv, metrics } = require("@stratum/plugin-sdk");

transform("running_total", {
  version: "1.0.0",
  output: "i64",
  input: { value: "i64" },
  compute(rows) {
    return rows.map(({ value }) => {
      const prev = parseInt(kv.get("sum") || "0", 10);
      const total = prev + Number(value);
      kv.set("sum", String(total));

      metrics.counter("rows_seen", 1);
      metrics.gauge("running_total", total);
      return total;
    });
  },
});
