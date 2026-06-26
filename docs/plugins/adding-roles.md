# Adding a New Plugin Role

This guide walks through what it takes to add a fifth plugin role to the
SDK (e.g. `#[stratum_aggregator]`, `#[stratum_router]`, …). It is the
companion doc to [`macro-expansion.md`](./macro-expansion.md) - read
that first if you want to see *what* the existing macros emit, then come
back here to see *how* to extend the system.

The four current roles - transform, filter, source, sink - all follow the
same pattern. Adding a fifth means filling in the same blanks one more
time. Nothing in this crate is special-cased per role; every role is just
a different combination of:

- a user-function signature,
- a host ABI export name,
- an optional config-loading branch in `__stratum_initialize`,
- a per-role role-specific payload type (input + output).

## Mental model: the five layers

When you add a role, you'll touch (roughly) five layers in this order:

1. **Wire format** - define the JSON shape the host sends and the plugin
   returns. Pick names for the input and output payload types.
2. **Host ABI** - decide the exported symbol name (e.g.
   `__stratum_aggregate`) and the function signature. Almost always
   `(ptr: u32, len: u32) -> u64` for "bytes in, packed (ptr, len) out".
3. **SDK types** (`stratum-plugin-sdk` crate) - add Rust structs for the
   input / output, with `from_json_bytes` / `to_json_bytes` helpers, and
   re-export them at the crate root.
4. **Macro generator** (this crate) - add `src/<role>.rs` that emits the
   ABI exports, wire it into `lib.rs` as a new `#[proc_macro_attribute]`.
5. **Host loader** (outside this crate, in the engine) - teach the host
   to recognize the new `"type": "<role>"` value in metadata JSON and
   invoke the new export.

This guide focuses on layers 1–4. Layer 5 lives in the engine and is
out of scope here - but check the host's existing per-role loader for
the pattern.

---

## Step 0: Pick the contract

Before writing any code, write the answers to these questions down
somewhere (a design doc, a comment, a PR description):

| Question | Example |
|----------|---------|
| Role name? | `aggregator` |
| Attribute macro name? | `#[stratum_aggregator]` |
| User function signature? | `fn(Vec<PluginInput>) -> PluginResult<AggregateResult>` |
| ABI export name? | `__stratum_aggregate` |
| Input wire type? | `AggregateBatch` (list of inputs + window metadata) |
| Output wire type? | `AggregateResult` |
| Needs config? | Yes - store reducer settings via `OnceLock` |
| Forbidden attributes? | `output` (return type is fixed) |
| Required attributes? | `name`, `version`, `input`, `output_schema` |

These answers determine every concrete decision below.

---

## Step 1: Add the SDK-side types

Inside `crates/sdk/stratum-plugin-sdk/src/`, create a module for the new
role's payload types - mirror what `source.rs` and `sink.rs` already do.

```rust
// crates/sdk/stratum-plugin-sdk/src/aggregator.rs
use crate::{PluginInput, PluginResult, Value};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub struct AggregateBatch {
    pub inputs: Vec<PluginInput>,
    pub window: WindowMetadata,
}

impl AggregateBatch {
    pub fn from_json_bytes(bytes: &[u8]) -> PluginResult<Self> {
        Ok(serde_json::from_slice(bytes)?)
    }
}

#[derive(Debug, Serialize)]
pub struct AggregateResult {
    pub value: Value,
    pub rows_consumed: u64,
}

impl AggregateResult {
    pub fn to_json_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("AggregateResult serializes")
    }
}

// If the role needs per-plugin config (like source/sink):
pub struct AggregatorConfig { /* ... */ }
impl AggregatorConfig {
    pub fn new(params: std::collections::HashMap<String, String>) -> Self { ... }
}
```

Then re-export at the crate root in `src/lib.rs`:

```rust
pub mod aggregator;
pub use aggregator::{AggregateBatch, AggregateResult, AggregatorConfig};
```

If the role needs config, add a `OnceLock` accessor alongside the
existing source/sink ones:

```rust
static AGGREGATOR_CONFIG: OnceLock<AggregatorConfig> = OnceLock::new();

pub fn aggregator_config() -> PluginResult<&'static AggregatorConfig> {
    AGGREGATOR_CONFIG
        .get()
        .ok_or_else(|| PluginError::internal("aggregator plugin not initialized"))
}

#[doc(hidden)]
pub fn __set_aggregator_config(cfg: AggregatorConfig) {
    let _ = AGGREGATOR_CONFIG.set(cfg);
}
```

---

## Step 2: Extend `InitBody` (only for a role-specific config accessor)

Every role's `__stratum_initialize` already parses the host config blob into the
general store readable via `stratum_plugin_sdk::config()` - that's what
`InitBody::None` does (transform/filter use it). **If `config()` is enough for
your role, reuse `InitBody::None` and skip this step.**

Add a new variant only if your role also exposes a *role-specific* accessor (the
way source/sink add `source_config()` / `sink_config()` on top of `config()`):

```rust
pub enum InitBody {
    None,
    Source,
    Sink,
    Aggregator, // new
}
```

…and a matching arm inside `shared_exports`. Mirror the source/sink arms: set the
general `PluginConfig` *and* your role-specific config from the same params.

```rust
InitBody::Aggregator => quote! {
    ::stratum_plugin_sdk::runtime::panic::install_panic_hook();
    let bytes = unsafe { ::stratum_plugin_sdk::runtime::abi::read_from_guest(ptr, len) };
    match ::stratum_plugin_sdk::runtime::parse_config(&bytes) {
        Ok(params) => {
            ::stratum_plugin_sdk::__set_plugin_config(
                ::stratum_plugin_sdk::PluginConfig::new(params.clone()),
            );
            ::stratum_plugin_sdk::__set_aggregator_config(
                ::stratum_plugin_sdk::AggregatorConfig::new(params),
            );
            0
        }
        Err(_) => 1,
    }
},
```

---

## Step 3: Write the generator

Create `src/aggregator.rs`. The skeleton below is what every existing
generator looks like - only the bodies of `metadata` and `role_entry`
change.

```rust
// src/aggregator.rs
use proc_macro2::TokenStream;
use quote::quote;
use syn::ItemFn;

use crate::abi::{InitBody, pack_result_tail, shared_exports};
use crate::common::{AttrArgs, build_metadata_json};

pub fn expand(attr: AttrArgs, user_fn: ItemFn) -> syn::Result<TokenStream> {
    // Always anchor diagnostics on the user's fn name.
    let span = user_fn.sig.ident.span();
    let name = attr.require_name(span)?;
    let version = attr.require_version(span)?;

    // Reject attributes that don't apply to this role.
    if attr.output.is_some() {
        return Err(syn::Error::new(
            span,
            "`output` is not valid on #[stratum_aggregator]",
        ));
    }

    // Build the embedded metadata JSON.
    let metadata = build_metadata_json(
        name,
        version,
        "aggregator", // <-- this string is what the host matches on
        &attr.input_schema,
        &attr.output_schema,
        None,
    );

    let shared = shared_exports(&metadata, InitBody::Aggregator);
    let user_ident = &user_fn.sig.ident;
    let tail = pack_result_tail();

    let role_entry = quote! {
        #[unsafe(no_mangle)]
        pub extern "C" fn __stratum_aggregate(ptr: u32, len: u32) -> u64 {
            let input_bytes = unsafe {
                ::stratum_plugin_sdk::runtime::abi::read_from_guest(ptr, len)
            };
            let result = ::std::panic::catch_unwind(
                || -> ::std::result::Result<
                    ::std::vec::Vec<u8>,
                    ::stratum_plugin_sdk::PluginError,
                > {
                    let batch =
                        ::stratum_plugin_sdk::AggregateBatch::from_json_bytes(&input_bytes)?;
                    let out: ::stratum_plugin_sdk::AggregateResult =
                        #user_ident(batch)?;
                    Ok(out.to_json_bytes())
                },
            );
            #tail
        }
    };

    Ok(quote! {
        #user_fn

        const _: () = {
            #shared
            #role_entry
        };
    })
}
```

Key things to keep consistent with existing roles:

- **Wrap in `const _: () = { ... };`** so generated `use`s don't leak.
- **Re-emit `#user_fn` unchanged** before the const block.
- **Use `pack_result_tail`** so all roles share identical error handling.
- **Wrap user code in `catch_unwind`** - never trust user code not to
  panic.
- **Use fully-qualified `::stratum_plugin_sdk::…` paths** inside `quote!`
  - the user's crate may not have a `use` for these types.
- **Anchor errors on `user_fn.sig.ident.span()`** so diagnostics point
  at the user's code.

---

## Step 4: Wire the macro into `lib.rs`

Add the module declaration and the attribute entry point:

```rust
// src/lib.rs
mod aggregator; // <-- new

#[proc_macro_attribute]
pub fn stratum_aggregator(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attr = parse_macro_input!(attr as AttrArgs);
    let user_fn = parse_macro_input!(item as syn::ItemFn);
    aggregator::expand(attr, user_fn)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
```

Then re-export it from the SDK crate (`stratum-plugin-sdk/src/lib.rs`):

```rust
pub use stratum_plugin_sdk_macros::{
    stratum_aggregator, // <-- new
    stratum_filter,
    stratum_sink,
    stratum_source,
    stratum_transform,
};
```

---

## Step 5: (Optional) Extend `AttrArgs`

If your role needs a new attribute key - say, `window_size = "5m"` -
add it to [`common::AttrArgs`](../../crates/sdk/stratum-plugin-sdk-macros/src/common.rs):

```rust
pub struct AttrArgs {
    pub name: Option<String>,
    pub version: Option<String>,
    pub output: Option<String>,
    pub input_schema: Vec<SchemaField>,
    pub output_schema: Vec<SchemaField>,
    pub prepare: Option<String>,      // existing (sink lifecycle)
    pub finalize: Option<String>,     // existing (sink lifecycle)
    pub window_size: Option<String>,  // <-- new
}
```

…and a matching arm in `Parse for AttrArgs`:

```rust
"window_size" => args.window_size = Some(parse_str(input)?),
```

If your role needs the new field in metadata JSON, extend
`build_metadata_json` to include it.

Prefer extending the shared `AttrArgs` over adding role-specific parsers
- keeping all attribute parsing in one place is what lets every role's
generator stay this short.

---

## Step 6: Write a smoke test

The fastest way to verify the macro produces something the host can load:

1. Create a temp plugin crate that uses your new macro.
2. Build it for `wasm32-wasip1`.
3. Run `wasm-objdump -x target/wasm32-wasip1/release/your_plugin.wasm`
   and confirm the export table contains:
   - `__STRATUM_PLUGIN_SENTINEL`
   - `__stratum_alloc`, `__stratum_dealloc`
   - `__stratum_metadata`, `__stratum_initialize`, `__stratum_shutdown`
   - your new role-specific export(s)
4. Run `strings` on the `.wasm` and confirm the embedded metadata JSON
   contains your role name (`"type":"aggregator"`).

That's enough to know the macro is well-formed. End-to-end testing
requires the host changes (layer 5).

---

## Step 7: Update the host

Outside this crate, the engine needs to:

- Recognize `"type": "aggregator"` in metadata JSON.
- Know to call `__stratum_aggregate` (not `__stratum_transform`).
- Know what to send and what to expect back on the wire.

Where this lives depends on the engine's plugin loader. Grep for the
existing strings `"transform"`, `"filter"`, `"source"`, `"sink"` to
find every place that needs a new arm.

---

## Checklist

When you're done, you should have touched:

- [ ] `stratum-plugin-sdk/src/<role>.rs` - payload types
- [ ] `stratum-plugin-sdk/src/lib.rs` - re-exports + optional `OnceLock`
- [ ] `stratum-plugin-sdk-macros/src/abi.rs` - new `InitBody` variant
      (only if the role needs config)
- [ ] `stratum-plugin-sdk-macros/src/common.rs` - new attribute keys
      (only if the role takes any)
- [ ] `stratum-plugin-sdk-macros/src/<role>.rs` - generator
- [ ] `stratum-plugin-sdk-macros/src/lib.rs` - `mod <role>;` and
      `#[proc_macro_attribute] pub fn stratum_<role>`
- [ ] Engine host loader - new role recognition + dispatch
- [ ] Smoke test plugin and `wasm-objdump` verification

If all four existing roles still build with `cargo build` and your new
smoke plugin builds for `wasm32-wasip1`, you're done with the SDK side.
