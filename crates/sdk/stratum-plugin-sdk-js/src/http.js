"use strict";

// These three globals are installed by the runtime before user code runs.
// They wrap engine-wasm's HostCapabilities::http_client.
//   __host_http_request(method, urlJson, headersJson, bodyOrEmpty) -> responseJson
//   responseJson = {"status": 200, "headers": {...}, "body": "..."}
module.exports = {
    get(url, opts = {}) {
        const resp = globalThis.__host_http_request("GET", url, JSON.stringify(opts.headers || {}), "");
        return JSON.parse(resp);
    },
    post(url, body, opts = {}) {
        const bodyStr = typeof body === "string" ? body : JSON.stringify(body);
        const resp = globalThis.__host_http_request("POST", url, JSON.stringify(opts.headers || {}), bodyStr);
        return JSON.parse(resp);
    },
    put(url, body, opts = {}) {
        const bodyStr = typeof body === "string" ? body : JSON.stringify(body);
        const resp = globalThis.__host_http_request("PUT", url, JSON.stringify(opts.headers || {}), bodyStr);
        return JSON.parse(resp);
    },
};