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

The host calls these exports over a small JSON wire protocol. You never write any
of that - you only write the handler body.

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

`fn(PluginInput) -> PluginResult<T>` where `T: Into<Value>`. `output` declares
the result type tag.

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
fn add(input: PluginInput) -> PluginResult<f64> {
    Ok(input.get_f64("a")? + input.get_f64("b")?)
}
```

### filter

`fn(PluginInput) -> PluginResult<FilterDecision>`. No `output`.

```rust
use stratum_plugin_sdk::{stratum_filter, FilterDecision, PluginInput, PluginResult};

#[stratum_filter(
    name = "positive",
    version = "1.0.0",
    input = [{ name = "value", type = "i64", nullable = false }]
)]
fn positive(input: PluginInput) -> PluginResult<FilterDecision> {
    if input.get_i64("value")? > 0 {
        Ok(FilterDecision::pass())
    } else {
        Ok(FilterDecision::reject("value must be positive"))
    }
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
- `allow_http` - `http_get` / `http_post`.
- `allow_kv` - `kv_get` / `kv_set`.
- `allow_metrics` - `metric_counter` / `metric_gauge`.

Calling a denied capability returns a `capability_denied` error.

## Errors and panics

Return `Err(PluginError::…)` for expected failures (`invalid_input`, `internal`,
…). Panics are caught and converted to a plugin error, so a bug in your handler
fails the row rather than tearing down the instance. How that row is handled
(skip, DLQ, abort) is controlled by the pipeline's `on_fail` / `on_error`.

## Verifying the build

```bash
stratum plugin inspect target/wasm32-wasip1/release/my_plugin.wasm
stratum plugin test    target/wasm32-wasip1/release/my_plugin.wasm --input '{"a":2,"b":3}'
```
