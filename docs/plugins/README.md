# Plugins

Stratum can be extended with **WebAssembly plugins** that run inside the
migration pipeline. A plugin is a sandboxed `.wasm` module the engine loads,
calls per row (or per batch / per page), and enforces resource limits on.

Plugins come in four **roles**:

| Role | Where it runs | Signature (conceptually) |
|------|---------------|--------------------------|
| **transform** | inside a pipeline's `select` | row field(s) -> one output value |
| **filter** | inside a pipeline's `validate` | row field(s) -> pass / reject |
| **source** | as the pipeline's `from` connection | cursor -> page of rows |
| **sink** | as the pipeline's `to` connection | batch of rows -> write result |

…and two **runtimes**:

- **Native** - a plugin written in Rust and compiled directly to
  `wasm32-wasip1`. Smallest and fastest. See [rust.md](./rust.md).
- **JavaScript** - a `.js` plugin bundled into a prebuilt **QuickJS** runtime
  WASM. Easiest to write, no Rust toolchain needed. See
  [javascript.md](./javascript.md).

Both runtimes implement the same host ABI and are loaded by the same engine, so
a plugin's role and behavior are identical regardless of language - only the
authoring experience and resource budget differ.

## Using a plugin in SMQL

Every plugin used by a pipeline is declared once with a `plugin` block, then
referenced by role.

```smql
plugin "to_upper" { path = "plugins/upper.js" }      # JS, compiled on first use
plugin "adder"    { path = "plugins/adder.wasm" }    # prebuilt native module
```

### transform - in `select`

```smql
select {
  id        = users.id
  loud_name = plugin.to_upper({ name: users.name })   # plugin output column
  total     = plugin.adder({ a: orders.price, b: orders.tax })
}
```

### filter - in `validate`

```smql
validate {
  rule "positive" {
    filter  = plugin.is_positive({ value: orders.amount })
    on_fail = skip          # skip (drop row) | fail (abort pipeline)
  }
}
```

### source / sink - as endpoints

A source or sink plugin is wired through a **connection** with `driver = "wasm"`
and a `plugin` property naming a declared plugin block:

```smql
connection "feed" { driver = "wasm" plugin = "my_source" }
connection "out"  { driver = "wasm" plugin = "my_sink" }

plugin "my_source" { path = "plugins/feed.wasm" }
plugin "my_sink"   { path = "plugins/sink.wasm" }

pipeline "ingest" {
  from { connection = connection.feed table = "events" }
  to   { connection = connection.out  table = "events" }
  select { id = events.id }
}
```

A WASM **source -> SQL destination** can create the destination table
automatically (`create_missing_tables = true`) - the schema is inferred from the
source plugin's declared `output` columns.

## Plugin configuration

A `config { ... }` block on the declaration is passed to the plugin at init
time. Values are strings; parse them inside the handler.

```smql
plugin "sampler" {
  path   = "plugins/sampler.wasm"
  config { rate = "0.2" }
}
```

Config reaches handlers in **both runtimes, all roles** - see the per-language
docs for the exact accessor (`config()` / `source_config()` in Rust; the handler
`config` argument in JS).

## Capabilities and resource limits

Plugins are sandboxed and denied everything by default. Grant capabilities and
override limits on the declaration:

```smql
plugin "geo" {
  path = "plugins/geo_enrich.js"

  allow_http         = true         # outbound HTTP (off by default)
  allow_log          = true         # host logging (on by default)
  memory_limit_bytes = 134217728    # 128 MiB
  fuel_limit         = 100000000    # ~instructions per call
  timeout_ms         = 30000        # wall-clock per call
}
```

The runtime enforces memory (`StoreLimits`), CPU (`fuel`), and wall-clock
(`epoch`) budgets per call. A plugin that exceeds them traps; the host stays up
and the row is routed to error handling. Defaults: transform/filter get a lean
row budget (64 MiB / 1M fuel / 1s); source/sink and JS plugins get a larger IO
budget (128 MiB / 100M fuel / 30s) because the QuickJS boot needs more headroom.

## CLI

```bash
# Compile a JS plugin to WASM (otherwise done automatically on apply/plan)
stratum plugin compile plugins/upper.js -o plugins/upper.wasm

# Print a plugin's metadata (name, version, role, schema)
stratum plugin inspect plugins/upper.wasm

# Validate every plugin referenced by an SMQL config (offline, no DB)
stratum plugin validate -c migration.smql

# Run a plugin once with sample input
stratum plugin test plugins/upper.wasm --input '{"name":"ada"}'
stratum plugin test plugins/feed.wasm  --mode source --json
stratum plugin test plugins/sink.wasm  --mode sink --input '[{"id":1},{"id":2}]'
```

`plugin validate` cross-checks each plugin's declared input schema and role
against how the pipelines use it, without touching a database.

## Authoring

- **[rust.md](./rust.md)** - write a native plugin with the `#[stratum_*]` macros.
- **[javascript.md](./javascript.md)** - write a JS plugin and how the QuickJS
  runtime works.

Runnable examples live in [`examples/plugins/`](../../examples/plugins/).

### Extending the SDK (maintainers)

- **[adding-roles.md](./adding-roles.md)** - add a new plugin role to the SDK.
- **[macro-expansion.md](./macro-expansion.md)** - what the `#[stratum_*]`
  attribute macros expand to.
