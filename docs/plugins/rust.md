# Native (Rust) plugins

A native plugin is an ordinary Rust crate compiled to `wasm32-wasip1`. You write
one function and annotate it with a `#[stratum_*]` attribute macro from
`stratum-plugin-sdk`; the macro emits the full host ABI around it. This is the
smallest, fastest plugin form - no JavaScript engine is embedded.

## How it works

The SDK (`stratum-plugin-sdk`) provides the value types, typed accessors, and the
role macros (`#[stratum_transform]`, `#[stratum_filter]`, `#[stratum_source]`,
`#[stratum_sink]`). Each macro generates:

- a **sentinel** symbol (defining two role macros in one crate is a link error -
  one role per module),
- host **allocator** hooks (`__stratum_alloc` / `__stratum_dealloc`) so the host
  can hand bytes into the plugin's linear memory,
- a **metadata** export (`__stratum_metadata`) - the name/version/role/schema
  baked in as JSON at compile time,
- an **initialize** export that parses the host-supplied `config` blob,
- the **role entry point** (`__stratum_transform`, `__stratum_read_page`, …)
  that decodes the wire payload, calls your function inside `catch_unwind` (a
  panic becomes a clean error, not an instance teardown), and encodes the result.

The host calls these exports over a batch-native wire protocol. Transform and
filter are invoked **once per batch**: the entry point decodes the whole batch
into a `Vec<PluginInput>`, hands it to your function, and encodes the `Vec` of
results you return. Native transform/filter plugins use the compact binary
`columnar_v1` format at the boundary (baked into the plugin's metadata as
`exchange_format`); source/sink use JSON. You never write any of that - you only
write the handler body.

For the gory details see [macro-expansion.md](./macro-expansion.md).

## Crate setup

```toml
# Cargo.toml
[package]
name = "my_plugin"
version = "0.1.0"
edition = "2024"

[lib]
crate-type = ["cdylib"]      # produces a .wasm cdylib

[dependencies]
stratum-plugin-sdk = "…"     # path or version
```

Build:

```bash
rustup target add wasm32-wasip1          # once
cargo build --target wasm32-wasip1 --release
# -> target/wasm32-wasip1/release/my_plugin.wasm
```

Point a `plugin` block at the resulting `.wasm`:

```smql
plugin "my_plugin" { path = "target/wasm32-wasip1/release/my_plugin.wasm" }
```

## The four roles

### transform

`fn(Vec<PluginInput>) -> PluginResult<Vec<T>>` where `T: Into<Value>`. The
handler receives the **whole batch** and returns one output per input, in order
(you own the loop). `output` declares the result type tag.

```rust
use stratum_plugin_sdk::{stratum_transform, PluginInput, PluginResult};

#[stratum_transform(
    name = "adder",
    version = "1.0.0",
    output = "f64",
    input = [
        { name = "a", type = "f64", nullable = false },
        { name = "b", type = "f64", nullable = false },
    ]
)]
fn add(inputs: Vec<PluginInput>) -> PluginResult<Vec<f64>> {
    inputs
        .iter()
        .map(|input| Ok(input.get_f64("a")? + input.get_f64("b")?))
        .collect()
}
```

The `Vec<PluginInput>` signature is deliberate: it lets a plugin do real
batch-level work (vectorized math, a single shared HTTP round-trip for the whole
batch, etc.). For a trivial per-row transform, `iter().map(...).collect()` is the
idiom - the length of the returned `Vec` must equal the number of inputs, or the
host rejects the batch.

### filter

`fn(Vec<PluginInput>) -> PluginResult<Vec<FilterDecision>>` - one decision per
input, in order. No `output`.

```rust
use stratum_plugin_sdk::{stratum_filter, FilterDecision, PluginInput, PluginResult};

#[stratum_filter(
    name = "positive",
    version = "1.0.0",
    input = [{ name = "value", type = "i64", nullable = false }]
)]
fn positive(inputs: Vec<PluginInput>) -> PluginResult<Vec<FilterDecision>> {
    inputs
        .iter()
        .map(|input| {
            Ok(if input.get_i64("value")? > 0 {
                FilterDecision::pass()
            } else {
                FilterDecision::reject("value must be positive")
            })
        })
        .collect()
}
```

### source

`fn(Option<String>) -> PluginResult<SourcePage>` - the argument is the cursor
(opaque string; `None` on the first call). Declare the rows you emit with
`output_schema`. Return the page plus the next cursor and a `has_more` flag so
the host knows when to stop.

```rust
use stratum_plugin_sdk::{stratum_source, source_config, PluginResult, Record, SourcePage};

#[stratum_source(
    name = "counter",
    version = "1.0.0",
    output_schema = [
        { name = "id",    type = "i64",    nullable = false },
        { name = "label", type = "string", nullable = false },
    ]
)]
fn read_page(cursor: Option<String>) -> PluginResult<SourcePage> {
    let total: i64 = source_config()?.get("total").and_then(|s| s.parse().ok()).unwrap_or(10);
    let page_size = 3;
    let start: i64 = cursor.as_deref().and_then(|s| s.parse().ok()).unwrap_or(0);
    let end = (start + page_size).min(total);

    let mut records = Vec::new();
    for i in start..end {
        let mut row = Record::with_capacity(2);
        row.set("id", i);
        row.set("label", format!("row-{i}"));
        records.push(row);
    }
    let has_more = end < total;
    Ok(SourcePage {
        records,
        next_cursor: has_more.then(|| end.to_string()),
        has_more,
    })
}
```

### sink

`fn(PluginBatch) -> PluginResult<WriteResult>`. Declare the columns you consume
with `input`. Optional `prepare` / `finalize` lifecycle hooks run once before the
first batch and once after the last.

```rust
use stratum_plugin_sdk::{stratum_sink, PluginBatch, PluginResult, WriteResult};

#[stratum_sink(
    name = "counter_sink",
    version = "1.0.0",
    input = [{ name = "id", type = "i64", nullable = false }],
    finalize = "flush"
)]
fn write_batch(batch: PluginBatch) -> PluginResult<WriteResult> {
    Ok(WriteResult::new(batch.len() as u64))
}

fn flush() -> PluginResult<()> {
    Ok(())
}
```

## Reading config

The `config { ... }` block on the declaration is delivered at init. Every role
can read the general store; source/sink also get a role-specific accessor.

```rust
use stratum_plugin_sdk::config;

let rate: f64 = config().get("rate").and_then(|s| s.parse().ok()).unwrap_or(1.0);
```

| Role | Accessor |
|------|----------|
| any | `stratum_plugin_sdk::config()` |
| source | `source_config()` (or `config()`) |
| sink | `sink_config()` (or `config()`); `prepare`/`finalize` fns read `config()` |

## Capabilities

Host capabilities are off by default and gated by the `plugin` declaration. The
SDK exposes them only when granted:

- `allow_log` (on by default) - `log_info` / `log_warn` / `log_error` / `log_debug`.
- `allow_http` - `http_get` / `http_post`. Returns the real response status and
  body (including for 4xx/5xx). Guarded regardless of the grant: link-local /
  cloud-metadata hosts (169.254.0.0/16, fe80::/10) are always refused, requests
  time out after 30s, and responses over 16 MiB are rejected rather than
  truncated. Narrow the reachable hosts with `allow_http_hosts` (see below).
- `allow_http_hosts` - optional list of hosts (exact match, case-insensitive,
  port ignored) that `http_request` may reach. Empty = any non-link-local host;
  non-empty = only the listed hosts. Ignored unless `allow_http` is set.
- `allow_kv` - `kv_get` / `kv_set`. An **instance-scoped scratch** store: it
  lives for the plugin instance and carries state between row/batch calls, but is
  **not** persisted to disk or shared across runs.
- `allow_metrics` - `metric_counter` / `metric_gauge` (emitted on the host's
  `plugin::metrics` tracing target).
- `allow_env` - list of environment-variable **names** the plugin may read via
  WASI. Values are resolved from the run's environment (the `EnvContext`, so
  `.env`-file variables count, not just the process environment); a name that
  isn't set is simply not exposed. Only the listed names are visible - nothing
  else from the host environment leaks in.
- `allow_fs_read` / `allow_fs_write` - lists of host directories preopened for
  the plugin via WASI (read-only / read-write). Each directory **must already
  exist** at run time; a missing one fails instantiation loudly rather than
  silently granting nothing. Only the listed directories are reachable.

A denied HTTP/KV call is inert (HTTP returns a `capability_denied` error to the
plugin; `kv_get` returns `None`, `kv_set` is a no-op); denied metrics calls are
no-ops; ungranted env vars and directories are simply absent from the sandbox.

## Errors and panics

Return `Err(PluginError::…)` for expected failures (`invalid_input`, `internal`,
…). Panics are caught and converted to a plugin error, so a bug in your handler
fails the batch rather than tearing down the instance. Note that transform/filter
handlers return one result per row but a single `Err` (or panic) fails the whole
batch - validate individual rows and encode a per-row verdict (a
`FilterDecision::reject`, a sentinel output value) when you want to reject a row
without failing its neighbors. How a failure is handled (skip, DLQ, abort) is
controlled by the check's `action` / the pipeline's `on_error`.

## Verifying the build

```bash
stratum plugin inspect target/wasm32-wasip1/release/my_plugin.wasm
# --input is a JSON array of rows (a batch); one output is printed per row.
stratum plugin test    target/wasm32-wasip1/release/my_plugin.wasm --input '[{"a":2,"b":3},{"a":10,"b":1}]'
```

The runnable batch-native examples in
[`benchmarks/plugins/rust/`](../../benchmarks/plugins/rust/) (`order_net`, a
transform, and `order_ok`, a filter) show the same `Vec<PluginInput>` pattern
against a real benchmark pipeline.
