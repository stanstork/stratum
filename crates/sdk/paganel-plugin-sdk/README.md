# paganel-plugin-sdk

Write [Paganel](https://github.com/stanstork/paganel) plugins in Rust.

Paganel is a data migration engine. A plugin runs inside its WASM sandbox and
can act as a **transform** (compute a column), a **filter** (accept or reject a
row), a **source** (produce rows) or a **sink** (consume them). This crate gives
you the types and the attribute macros; the macros emit the whole ABI, so you
write an ordinary Rust function.

```toml
[dependencies]
paganel-plugin-sdk = "0.1"

[lib]
crate-type = ["cdylib"]
```

```rust,no_run
use paganel_plugin_sdk::{paganel_transform, PluginInput, PluginResult};

#[paganel_transform(
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

Money needs a different shape. Tag both the input and the `output` as
`decimal`. With `f64` the amount travels through a float, `plan` warns that
precision may be lost, and the destination column ends up floating point.

That costs two extra steps next to the example above: add `bigdecimal = "0.4"`
to your `Cargo.toml`, because the SDK hands you `BigDecimal` values without
exporting the type itself, and return `Vec<Value>` with each result wrapped in
`Value::Decimal`, because the plain `Vec<T>` return only covers primitives.

```rust,no_run
use bigdecimal::BigDecimal;
use paganel_plugin_sdk::{paganel_transform, PluginInput, PluginResult, Value};

#[paganel_transform(
    name = "net_after_fee",
    version = "1.0.0",
    output = "decimal",
    input = [{ name = "amount", type = "decimal", nullable = false }]
)]
fn net_after_fee(inputs: Vec<PluginInput>) -> PluginResult<Vec<Value>> {
    let rate: BigDecimal = "0.971".parse().expect("valid literal");
    inputs
        .iter()
        .map(|row| Ok(Value::Decimal((row.get_decimal("amount")? * &rate).round(2))))
        .collect()
}
```

Build it for the sandbox and point a pipeline at the result:

```bash
cargo build --target wasm32-wasip1 --release
```

```ppl
plugin "adder" { path = "target/wasm32-wasip1/release/adder.wasm" }

pipeline "orders" {
  from { connection = connection.src table = "orders" }
  to   { connection = connection.dst table = "orders" }

  select {
    id    = orders.id
    total = plugin.adder({ a: orders.net, b: orders.tax })
  }
}
```

A plugin is called once per batch, not once per row: the macro hands your
function the whole batch and the host crosses the WASM boundary a single time.
Plugins run with fuel, memory and timeout caps and no filesystem or network
access unless a pipeline grants them.

## Docs

- [Writing a Rust plugin](https://github.com/stanstork/paganel/blob/main/docs/plugins/rust.md) - roles, capabilities, testing
- [Plugin overview](https://github.com/stanstork/paganel/blob/main/docs/plugins/README.md) - runtimes, limits, CLI
- [API reference](https://docs.rs/paganel-plugin-sdk)

## License

MIT. The Paganel engine itself is AGPL-3.0-or-later; this SDK is permissive so
your plugin's license stays yours.
