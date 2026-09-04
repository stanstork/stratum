# JS test plugins

JavaScript counterparts of the Rust test plugins in `../rust/`. Each file here
mirrors the behavior of the like-named Rust crate so the parity tests can assert
the JS runtime produces identical output for identical input.

| File                   | Rust counterpart        | Exercises                          |
| ---------------------- | ----------------------- | ---------------------------------- |
| `test_transform.js`    | `test_transform`        | transform role (`a + b`)           |
| `test_filter.js`       | `test_filter`           | filter role (positive-only)        |
| `test_source.js`       | `test_source`           | source role (paginated synthetic)  |
| `test_infinite_loop.js`| `test_infinite_loop`    | fuel limit on a runaway plugin     |
| `test_memory_hog.js`   | `test_memory_hog`       | memory limit on an allocating plugin |

## Building fixtures

Unlike the Rust plugins (one `cargo build --target wasm32-wasip1` each), a JS
plugin is compiled by embedding it into the pre-built JS runtime WASM. The
`build_fixtures.sh` script automates this - its `js` branch rebuilds the runtime,
then compiles every plugin here:

```bash
../../build_fixtures.sh js      # JS only  (or `all` for Rust + JS)
```

That branch:

1. rebuilds `paganel-plugin-js-runtime` into the compiler crate's committed asset
   (`crates/sdk/paganel-plugin-compiler/assets/paganel-plugin-js-runtime.wasm`),
   which the compiler embeds,
2. writes a wrapper that runs `npx esbuild`, and
3. runs `pag plugin compile <file>.js -o ../../fixtures/<file>_js.wasm` for each.

The per-file command (what the loop runs) is:

```bash
pag plugin compile test_transform.js \
    -o ../../fixtures/test_transform_js.wasm \
    --esbuild-path <npx-esbuild-wrapper> \
    --runtime-wasm <freshly-built runtime asset>
```

`@paganel/plugin-sdk` needs no `node_modules` setup: the compiler bundles the
SDK it embeds and hands it to esbuild (`--runtime-wasm` here just pins the
freshly-rebuilt runtime instead of the embedded default).

Under the hood this is `paganel-plugin-compiler`, which bundles the JS with the
`@paganel/plugin-sdk` package via esbuild, extracts metadata by running the
bundle in QuickJS, and patches both into the runtime's data segments. The
compiled `*_js.wasm` files load through the same `WasmEngine` as the Rust
fixtures, so the parity tests run them side by side.
