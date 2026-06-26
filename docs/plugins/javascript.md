# JavaScript plugins

A JavaScript plugin is a single `.js` file that registers a handler with the
`@stratum/plugin-sdk` package. You don't need a Rust toolchain - Stratum bundles
your code into a prebuilt **QuickJS** WebAssembly runtime and runs it through the
same engine as native plugins.

## How it works (the QuickJS runtime)

JavaScript can't be loaded by the WASM engine directly, so a `.js` plugin is
turned into a `.wasm` module in three steps (handled by
`stratum-plugin-compiler`):

1. **Bundle.** `esbuild` bundles your file together with `@stratum/plugin-sdk`
   into a single self-contained IIFE script. The SDK's registry attaches its
   metadata/dispatch hooks to `globalThis` as a load-time side effect, so the
   bundle is self-exposing.
2. **Extract metadata.** The bundle is executed once inside an in-process QuickJS
   context to read back what it registered - name, version, role, and input /
   output schema - producing the same metadata JSON a native plugin bakes in at
   compile time.
3. **Patch.** The bundled JS and the metadata JSON are written into two reserved
   data segments of a **prebuilt runtime WASM**
   (`stratum-plugin-js-runtime`, a QuickJS interpreter compiled to
   `wasm32-wasip1` via `rquickjs`). The result is an ordinary `.wasm` plugin.

At migration time the engine loads that `.wasm` like any other: on
`__stratum_initialize` the runtime boots a QuickJS context, evaluates the
embedded bundle (registering your handler), and applies the config; each
role call (`__stratum_transform`, `__stratum_read_page`, …) marshals the row
to/from JS values and invokes your handler. So the only differences from a native
plugin are the QuickJS boot cost and a larger default resource budget.

This means **every JS plugin embeds its own copy of the QuickJS runtime**
(~5 MB). That's expected - the compiled `.wasm` is self-contained.

### Compilation is automatic

You normally never run the compiler yourself. When a `plugin` block points at a
`.js` file, `stratum plan` / `stratum apply` compile it on demand and cache the
result under `~/.stratum/plugin-cache/` (keyed by a hash of the source + runtime,
so an unchanged plugin is only compiled once).

```smql
plugin "upper" { path = "plugins/upper.js" }   # compiled + cached on first use
```

To compile ahead of time (e.g. for CI or to ship a `.wasm`):

```bash
stratum plugin compile plugins/upper.js -o plugins/upper.wasm
```

### Requirements

JS compilation needs **esbuild** (or Node.js for `npx esbuild`) available at
plan/apply time, and `@stratum/plugin-sdk` resolvable from the plugin's
directory (esbuild bundles it in). Prebuilt `.wasm` plugins have neither
requirement. See [`examples/plugins/README.md`](../../examples/plugins/README.md)
for the `node_modules` setup used by the examples.

## Authoring API

`require("@stratum/plugin-sdk")` exposes `transform`, `filter`, `source`, `sink`,
plus the `http` and `log` capability helpers. Call exactly one role registrar per
file.

### transform

```js
const { transform } = require("@stratum/plugin-sdk");

transform("adder", {
  version: "1.0.0",
  input:  { a: "f64", b: "f64" },
  output: "f64",
  compute({ a, b }, config) {     // 2nd arg = plugin config (strings)
    return a + b;
  },
});
```

### filter

```js
const { filter } = require("@stratum/plugin-sdk");

filter("positive", {
  version: "1.0.0",
  input: { value: "i64" },
  evaluate({ value }, config) {
    return value > 0 ? { pass: true } : { pass: false, reason: "must be positive" };
  },
});
```

### source

```js
const { source } = require("@stratum/plugin-sdk");

source("counter", {
  version: "1.0.0",
  output: { id: "i64", label: "string" },
  readPage(config, cursor) {            // cursor: opaque string | null
    const total = parseInt(config.total ?? "10", 10);
    const start = cursor != null ? parseInt(cursor, 10) : 0;
    const end = Math.min(start + 3, total);
    const records = [];
    for (let i = start; i < end; i++) records.push({ id: i, label: `row-${i}` });
    const hasMore = end < total;
    return { records, next_cursor: hasMore ? String(end) : null, has_more: hasMore };
  },
});
```

### sink

```js
const { sink, log } = require("@stratum/plugin-sdk");

let total = 0;
sink("counter_sink", {
  version: "1.0.0",
  input: { id: "i64" },
  prepare(config)  { total = 0; },
  writeBatch(config, { records }) {
    total += records.length;
    return { rows_written: records.length };
  },
  finalize(config) { log.info(`wrote ${total} rows`); },
});
```

## Config

The `config { ... }` block on the declaration is delivered to the handler:

- `source` / `sink`: as the **first** argument (`readPage(config, cursor)`,
  `writeBatch(config, batch)`, `prepare(config)`, `finalize(config)`).
- `transform` / `filter`: as the **second** argument (`compute(row, config)`,
  `evaluate(row, config)`).

Values are strings - parse them in the handler.

## Capabilities

- `log.info` / `log.warn` / `log.error` / `log.debug` - host logging (on by
  default via `allow_log`).
- `http.get` / `http.post` - outbound HTTP, only when the declaration sets
  `allow_http = true`.

## Native vs. JavaScript - which to use

| | Native (Rust) | JavaScript |
|--|---------------|------------|
| Toolchain | Rust + `wasm32-wasip1` | esbuild / Node |
| Module size | small (KBs–100s KB) | ~5 MB (embeds QuickJS) |
| Per-call cost | lowest | QuickJS overhead |
| Default budget | lean row budget | larger IO budget (QuickJS boot) |
| Best for | hot paths, heavy logic | quick logic, no Rust setup |

Both expose identical roles and config; pick by toolchain preference and
performance needs. See [rust.md](./rust.md) for the native path.
