// Sink - counts rows and logs via the host logging capability (`allow_log`,
// on by default). Uses the optional prepare/finalize lifecycle hooks.
// Test:  plugin test log_sink.js --mode sink --input '[{"id":1},{"id":2}]'
//        -> rows_written=2, plus "info" logs incl. a FINALIZE line.
const { sink, log } = require("@stratum/plugin-sdk");

let total = 0;

sink("log_sink", {
  version: "1.0.0",
  input: { id: "i64" },
  prepare() {
    total = 0;
    log.info("log_sink: prepare");
  },
  writeBatch(_config, { records }) {
    total += records.length;
    log.info(`log_sink: wrote ${records.length} (running total ${total})`);
    return { rows_written: records.length };
  },
  finalize() {
    log.info(`log_sink: finalize, ${total} rows total`);
  },
});
