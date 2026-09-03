"use strict";

// Instance-scoped scratch key-value store, gated by `allow_kv`. Values live for
// the plugin instance (across row/batch calls) and are NOT persisted. `get`
// returns null when the key is absent or the capability is denied.
module.exports = {
    get: (key) => globalThis.__host_kv_get(String(key)) ?? null,
    set: (key, value) => globalThis.__host_kv_set(String(key), String(value)),
};
