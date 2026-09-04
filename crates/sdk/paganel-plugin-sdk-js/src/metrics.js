"use strict";

// Custom metrics, gated by `allow_metrics`. Emitted on the host's
// `plugin::metrics` tracing target. No-op when the capability is denied.
// `counter` values are truncated to an integer on the host.
module.exports = {
    counter: (name, value) => globalThis.__host_metric_counter(String(name), Number(value)),
    gauge: (name, value) => globalThis.__host_metric_gauge(String(name), Number(value)),
};
