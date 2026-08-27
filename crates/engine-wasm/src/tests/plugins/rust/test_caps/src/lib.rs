//! A transform plugin that exercises every host capability, so the integration
//! tests can assert them end to end.

use stratum_plugin_sdk::{
    PluginInput, PluginResult, env_get, fs_read_to_string, http_get, kv_get, kv_set,
    metric_counter, stratum_transform,
};

#[stratum_transform(
    name = "test_caps",
    version = "1.0.0",
    output = "string",
    input = [ { name = "seed", type = "string", nullable = false } ]
)]
fn probe(inputs: Vec<PluginInput>) -> PluginResult<Vec<String>> {
    inputs
        .iter()
        .map(|_| {
            // kv: increment a per-instance counter.
            let prev = kv_get("n")?
                .and_then(|b| String::from_utf8(b).ok())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);
            let n = prev + 1;
            kv_set("n", n.to_string().as_bytes())?;

            // metrics: fire-and-forget (a no-op when ungranted).
            metric_counter("caps_probe_rows", 1);

            // env: read a granted variable.
            let env = env_get("CAPS_ENV").unwrap_or_else(|| "-".to_string());

            // fs: read the file whose path is itself a granted env var.
            let fs = env_get("CAPS_FILE")
                .and_then(|p| fs_read_to_string(&p).ok())
                .map(|s| s.trim().to_string())
                .unwrap_or_else(|| "-".to_string());

            // http: GET a URL from a granted env var, report "<status>:<body>".
            let http = env_get("CAPS_HTTP_URL")
                .and_then(|u| http_get(&u).ok())
                .map(|r| format!("{}:{}", r.status, String::from_utf8_lossy(&r.body).trim()))
                .unwrap_or_else(|| "-".to_string());

            Ok(format!("kv={n};env={env};fs={fs};http={http}"))
        })
        .collect()
}
