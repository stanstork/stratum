"use strict";

module.exports = {
    info: (m) => globalThis.__host_log("info", m),
    warn: (m) => globalThis.__host_log("warn", m),
    error: (m) => globalThis.__host_log("error", m),
    debug: (m) => globalThis.__host_log("debug", m),
};