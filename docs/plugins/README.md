# Plugins

Stratum can be extended with **WebAssembly plugins** that run inside the
migration pipeline. A plugin is a sandboxed `.wasm` module the engine loads,
calls once per **batch** (or per page), and enforces resource limits on.

Plugins come in four **roles**:

| Role | Where it runs | Signature (conceptually) |
|------|---------------|--------------------------|
| **transform** | inside a pipeline's `select` | batch of rows -> one output value per row |
| **filter** | inside a pipeline's `validate` | batch of rows -> pass / reject per row |
| **source** | as the pipeline's `from` connection | cursor -> page of rows |
| **sink** | as the pipeline's `to` connection | batch of rows -> write result |

All four roles are wired into the pipeline: transform/filter run inside a
pipeline's `select`/`validate`, and source/sink run as endpoints (a `connection`
with `driver = "wasm"`), with checkpoint/resume support.

…and two **runtimes**:

- **Native** - a plugin written in Rust and compiled directly to
  `wasm32-wasip1`. Smallest and fastest. See [rust.md](./rust.md).
- **JavaScript** - a `.js` plugin bundled into a prebuilt **QuickJS** runtime
  WASM. Easiest to write, no Rust toolchain needed. See
  [javascript.md](./javascript.md).

Both runtimes implement the same host ABI and are loaded by the same engine, so
a plugin's role and behavior are identical regardless of language - only the
authoring experience and resource budget differ.

## The ABI is batch-native

Transform and filter plugins are called **once per batch**, not once per row.
The host serializes the whole batch, crosses the WASM boundary a single time,
and the guest returns exactly one result per input row (in order). This
amortizes the boundary crossing (alloc / serialize / call / deserialize) over
the entire batch instead of paying it per row - the dominant cost in a trivial
transform. Both authoring SDKs surface this directly: a Rust handler takes
`Vec<PluginInput>` and returns a `Vec` of outputs; a JS handler takes the array
of rows and returns an array of results. The author owns the loop over the
batch.

Two binary/text **wire formats** carry the batch, negotiated per plugin via the
metadata `exchange_format` field:

- **`columnar_v1`** - the default for native Rust transform/filter plugins. A
  compact binary frame of typed column arrays plus a validity bitmap (Arrow-ish).
  The host and guest codecs are byte-identical mirrors. Much faster than JSON at
  the boundary.
- **`json_v1` (flat)** - used by JS transform/filter plugins. Flat JSON arrays of
  bare values, with **no** `{type, value}` envelope (JS is dynamically typed, so
  scalars need no type tag).

The *enveloped* `{type, value}` JSON form is still in use: source/sink plugins
exchange records that way, and `columnar_v1` falls back to a per-cell enveloped
JSON encoding (`TAG_CELL`) for columns whose value types have no native columnar
representation (mixed or host-only variants).

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
  assert "positive" {
    check  = plugin.is_positive({ value: orders.amount })
    action = skip          # skip (drop row) | fail (abort pipeline) | warn
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
and the failed batch is routed to error handling. Because transform/filter calls
carry a whole batch, their fuel / output-size / wall-clock limits are treated as
**per-row rates and scaled by the batch's row count** (capped at 256 MiB output
and 30s wall-clock). Defaults: native transform/filter get 128 MiB memory with a
per-row rate of 1M fuel / 1 MiB output / 1s; source/sink and JS plugins get a
larger flat IO budget (128 MiB / 100M fuel / 16 MiB output / 30s) because the
QuickJS boot needs more headroom.

## CLI

```bash
# Compile a JS plugin to WASM (otherwise done automatically on apply/plan)
stratum plugin compile plugins/upper.js -o plugins/upper.wasm

# Print a plugin's metadata (name, version, role, schema)
stratum plugin inspect plugins/upper.wasm

# Validate every plugin referenced by an SMQL config (offline, no DB)
stratum plugin validate -c migration.smql

# Run a plugin over a batch of sample rows (input is a JSON ARRAY of rows;
# a single object is accepted as a one-row batch)
stratum plugin test plugins/upper.wasm --input '[{"name":"ada"},{"name":"grace"}]'
stratum plugin test plugins/order_ok.wasm --mode filter --input '[{"amount":10},{"amount":-5}]'
stratum plugin test plugins/feed.wasm  --mode source --json
stratum plugin test plugins/sink.wasm  --mode sink --input '[{"id":1},{"id":2}]'
```

`plugin test` prints one result per input row: transform emits the output value
per row (`{"values":[...]}` with `--json`), filter emits `PASS`/`REJECT` per row
(`{"passes":[true,false]}` with `--json`), source reports the page it produced,
and sink reports `rows_written`.

`plugin validate` cross-checks each plugin's declared input schema and role
against how the pipelines use it, without touching a database.

## Performance

The batch-native ABI plus the `columnar_v1` wire format put native plugins close
to built-in expressions. On a 10M-row MySQL -> PostgreSQL benchmark: a native
Rust transform plugin sustains ~486k rows/s and a native Rust filter ~704k
rows/s (columnar wire, near-native). The equivalent JavaScript plugin/filter
runs at ~120-127k rows/s - that floor is the QuickJS interpreter itself, not the
plugin boundary. Pick native Rust for hot paths; reach for JS when the logic is
light and you'd rather skip the Rust toolchain.

## Authoring

- **[rust.md](./rust.md)** - write a native plugin with the `#[stratum_*]` macros.
- **[javascript.md](./javascript.md)** - write a JS plugin and how the QuickJS
  runtime works.

Runnable examples live in [`examples/plugins/`](../../examples/plugins/).

### Extending the SDK (maintainers)

- **[adding-roles.md](./adding-roles.md)** - add a new plugin role to the SDK.
- **[macro-expansion.md](./macro-expansion.md)** - what the `#[stratum_*]`
  attribute macros expand to.
