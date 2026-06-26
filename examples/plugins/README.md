# Plugin Examples

A spread of small JS plugins and SMQL configs for exercising the plugin system.

```
examples/plugins/
  js/         one plugin per file (compiled + cached on first use)
  configs/    SMQL wiring the plugins into pipelines
```

## Setup

The JS plugins `require("@stratum/plugin-sdk")`. esbuild resolves it from a
`node_modules` symlink - create it once:

```bash
mkdir -p examples/plugins/js/node_modules/@stratum
ln -sfn ../../../../../crates/sdk/stratum-plugin-sdk-js \
        examples/plugins/js/node_modules/@stratum/plugin-sdk
```

esbuild (or `npx esbuild`) must be on `PATH`.

```bash
alias s='cargo run -p cli --'   # or ./target/debug/cli
```

## JS plugins (`js/`)

| File | Role | What it shows | One-shot test |
|---|---|---|---|
| `upper.js` | transform | single string in/out | `s plugin test js/upper.js --input '{"name":"ada"}'` -> `String("ADA")` |
| `full_name.js` | transform | multi-field input | `--input '{"first":"Ada","last":"Lovelace"}'` -> `String("Ada Lovelace")` |
| `gross_to_net.js` | transform | two numeric inputs (mapped cols) | `--input '{"gross":100,"rate":0.2}'` -> `Float(80.0)` |
| `discount.js` | transform | reads **config** (2nd arg) | `--config-json <(echo '{"rate":"0.2"}')` `--input '{"price":100}'` -> `Float(80.0)` |
| `safe_divide.js` | transform | per-row error (throws on /0) | `--input '{"num":10,"den":0}'` -> surfaced plugin error |
| `email_valid.js` | filter | string/regex validation | `--input '{"email":"nope"}'` -> `REJECT` |
| `in_range.js` | filter | numeric bounds, two fields | `--input '{"value":50,"max":10}'` -> `REJECT` |
| `counter_source.js` | source | cursor paging | `s plugin test js/counter_source.js --json` -> 3 rows, `has_more=true` |
| `log_sink.js` | sink | host logging capability | `--mode sink --input '[{"id":1},{"id":2}]'` -> `rows_written=2` |
| `geo_enrich.js` | transform | **HTTP capability** (needs `allow_http`) | run via `configs/capabilities.smql` |
| `throws_on_init.js` | — | **negative**: throws at load | `s plugin inspect js/throws_on_init.js` -> clean compile error |

## Configs (`configs/`)

| File | Purpose | Run |
|---|---|---|
| `transforms.smql` | transforms in `select` (clean) | `s plugin validate -c …` ✓ / `s plan -c …` / `s apply -c …` |
| `filters.smql` | filters in `validate { rule … }` (clean) | `s plugin validate -c …` ✓ |
| `capabilities.smql` | HTTP grant + resource-limit overrides | toggle `allow_http` to test the gate |
| `diagnostics.smql` | **deliberately broken** — one fault per pipeline | `s plugin validate -c …` / `s plan -c …` -> expect errors |
| `source_endpoint.smql` | WASM plugin as the pipeline **source** (`driver = "wasm"`, cursor paging) | `s apply -c …` -> 7 rows, cursor `None->3->6->None` (no DB) |
| `sink_endpoint.smql` | WASM plugin as the pipeline **sink** + prepare/finalize lifecycle | `s apply -c …` -> prepare / wrote / finalize logs (no DB) |

`plugin validate` works offline (no DB). The endpoint configs
(`source_endpoint`/`sink_endpoint`) are also fully self-contained - no DB needed.
The transform/filter `plan`/`apply` configs introspect the source and default to
the Sakila MySQL->Postgres setup used by the other examples (override `MYSQL_URL` /
`POSTGRES_URL`).

> **Plugin endpoints vs. transform/filter.** A transform/filter plugin is called
> inside `select`/`validate` as `plugin.<name>(...)`. A *source/sink* plugin is a
> `connection { driver = "wasm" plugin = "<name>" }` referenced from `from`/`to` -
> see `source_endpoint.smql` / `sink_endpoint.smql`.

### What `diagnostics.smql` should report

```
⚠ pipeline 'unknown_key': plugin 'upper' mapping references unknown input field 'bogus'
✗ pipeline 'role_mismatch': 'email_valid' is a Filter, but is used as a transform
✗ pipeline 'unmapped_field': plugin 'gross_to_net' input field 'rate' is not provided by its mapping
```

Its `type_mismatch` pipeline is a **`plan`-only** check (canonical types) - run
`s plan -c configs/diagnostics.smql` against a live DB to see
`PLUGIN_INPUT_TYPE_MISMATCH`.

## Plugin config

Values from the SMQL `plugin "x" { config { ... } }` block (or `plugin test
--config-json <file>`) reach handlers in **both runtimes, all roles**:

| Role | JS | Rust |
|---|---|---|
| source / sink | `readPage(config, cursor)` / `writeBatch(config, batch)` (1st arg) | `source_config()` / `sink_config()`, or `config()` |
| transform / filter | 2nd handler arg: `compute(row, config)` / `evaluate(row, config)` | `stratum_plugin_sdk::config()` |
| sink prepare/finalize | `prepare(config)` / `finalize(config)` | `#[stratum_sink(..., prepare = "fn", finalize = "fn")]`; the fns read `config()` |

```bash
echo '{"rate":"0.2"}' > /tmp/cfg.json
s plugin test js/gross_to_net.js --config-json /tmp/cfg.json --input '{"gross":100,"rate":0.2}'
```

`plugin test --config-json` takes a **file path** (use process substitution for
inline). Config values are strings; parse them in the handler.