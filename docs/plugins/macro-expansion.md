# Macro Expansion Example

This file shows what the attribute macros in this crate actually emit, so
you can reason about the generated code without running `cargo-expand`.

## Input the user writes

```rust
use stratum_plugin_sdk::{stratum_transform, PluginInput, PluginResult};

#[stratum_transform(
    name = "discount",
    version = "1.0.0",
    output = "f64",
    input = [{ name = "total", type = "f64", nullable = false }]
)]
fn calculate_discount(input: PluginInput) -> PluginResult<f64> {
    let total = input.get_f64("total")?;
    Ok(total * 0.9)
}
```

## What the compiler sees after expansion

```rust
// 1. The user function is re-emitted unchanged.
fn calculate_discount(input: PluginInput) -> PluginResult<f64> {
    let total = input.get_f64("total")?;
    Ok(total * 0.9)
}

// 2. Anonymous const block - keeps generated `use` items out of the
//    user's namespace, but `pub extern "C"` symbols still reach the linker.
const _: () = {
    // ---------- shared_exports() ----------

    // Sentinel: defining this symbol twice (i.e. two role macros in one
    // crate) is a linker error. One role per cdylib, enforced cheaply.
    #[doc(hidden)]
    #[unsafe(no_mangle)]
    pub static __STRATUM_PLUGIN_SENTINEL: u8 = 0;

    // Host allocator hooks - the host calls these to put bytes into our
    // linear memory before invoking us, and to free them after.
    #[unsafe(no_mangle)]
    pub extern "C" fn __stratum_alloc(size: u32) -> u32 {
        unsafe { ::stratum_plugin_sdk::runtime::abi::alloc_bytes(size) }
    }

    #[unsafe(no_mangle)]
    pub extern "C" fn __stratum_dealloc(ptr: u32, size: u32) {
        unsafe { ::stratum_plugin_sdk::runtime::abi::dealloc_bytes(ptr, size); }
    }

    // Metadata JSON baked in at compile time from the attribute args.
    static __STRATUM_METADATA_JSON: &str =
        "{\"name\":\"discount\",\"version\":\"1.0.0\",\"type\":\"transform\",\
          \"exchange_format\":\"json_v1\",\"runtime\":\"native\",\
          \"input_schema\":[{\"name\":\"total\",\"type\":\"f64\",\"nullable\":false}],\
          \"output_type\":\"f64\"}";

    #[unsafe(no_mangle)]
    pub extern "C" fn __stratum_metadata() -> u64 {
        let (p, l) = unsafe {
            ::stratum_plugin_sdk::runtime::abi::write_to_guest(
                __STRATUM_METADATA_JSON.as_bytes(),
            )
        };
        ::stratum_plugin_sdk::runtime::pack::pack(p, l)
    }

    // Init: installs the panic hook and parses the host config blob into the
    // general `config()` store. (Source/sink branches additionally stash a
    // role-specific `source_config()` / `sink_config()`.)
    #[unsafe(no_mangle)]
    pub extern "C" fn __stratum_initialize(ptr: u32, len: u32) -> u32 {
        let _ = (ptr, len);
        ::stratum_plugin_sdk::runtime::panic::install_panic_hook();
        let bytes = unsafe { ::stratum_plugin_sdk::runtime::abi::read_from_guest(ptr, len) };
        match ::stratum_plugin_sdk::runtime::parse_config(&bytes) {
            Ok(params) => {
                ::stratum_plugin_sdk::__set_plugin_config(
                    ::stratum_plugin_sdk::PluginConfig::new(params),
                );
                0
            }
            Err(_) => 1, // non-zero tells the host init failed
        }
    }

    #[unsafe(no_mangle)]
    pub extern "C" fn __stratum_shutdown() {}

    // ---------- role entry ----------

    #[unsafe(no_mangle)]
    pub extern "C" fn __stratum_transform(ptr: u32, len: u32) -> u64 {
        // Reclaim the bytes the host wrote into our linear memory.
        let input_bytes = unsafe {
            ::stratum_plugin_sdk::runtime::abi::read_from_guest(ptr, len)
        };

        // catch_unwind so a user panic doesn't tear down the WASM instance.
        let result = ::std::panic::catch_unwind(
            || -> ::std::result::Result<
                ::std::vec::Vec<u8>,
                ::stratum_plugin_sdk::PluginError,
            > {
                let input =
                    ::stratum_plugin_sdk::PluginInput::from_json_bytes(&input_bytes)?;
                // `.into()` lifts the user's f64 into Value automatically.
                let value: ::stratum_plugin_sdk::Value =
                    calculate_discount(input)?.into();
                Ok(::stratum_plugin_sdk::PluginOutput::new(value).to_json_bytes())
            },
        );

        // ---------- pack_result_tail() ----------
        let bytes = match result {
            Ok(Ok(b)) => b,
            Ok(Err(e)) => ::stratum_plugin_sdk::error::serialize_error(&e),
            Err(_) => {
                let msg = ::stratum_plugin_sdk::runtime::panic::take_panic()
                    .unwrap_or_else(|| "plugin panicked".to_string());
                ::stratum_plugin_sdk::error::serialize_error(
                    &::stratum_plugin_sdk::PluginError::panic(msg),
                )
            }
        };
        let (out_ptr, out_len) = unsafe {
            ::stratum_plugin_sdk::runtime::abi::write_to_guest(&bytes)
        };
        ::stratum_plugin_sdk::runtime::pack::pack(out_ptr, out_len)
    }
};
```

## How the other three roles differ

The shared block (sentinel, alloc, dealloc, metadata, init, shutdown) is the
same for all four roles, except `__stratum_initialize`'s body: every role parses
the config blob into the general `config()` store; source and sink *additionally*
stash a role-specific config. The role-specific entry points differ as follows.

### Filter (`#[stratum_filter]`)

Same shape as transform, but:

- Function renamed `__stratum_evaluate`
- No `.into()` - `FilterDecision` is concrete

```rust
let decision: FilterDecision = my_filter(input)?;
Ok(decision.to_json_bytes())
```

### Source (`#[stratum_source]`)

`__stratum_initialize`'s body becomes:

```rust
::stratum_plugin_sdk::runtime::panic::install_panic_hook();
let bytes = unsafe { ...::read_from_guest(ptr, len) };
match ::stratum_plugin_sdk::runtime::parse_config(&bytes) {
    Ok(params) => {
        // general config() for every role, plus the source-specific accessor
        ::stratum_plugin_sdk::__set_plugin_config(PluginConfig::new(params.clone()));
        ::stratum_plugin_sdk::__set_source_config(SourceConfig::new(params));
        0
    }
    Err(_) => 1,
}
```

Plus two role exports:

```rust
pub extern "C" fn __stratum_read_page(ptr: u32, len: u32) -> u64 {
    // ... catch_unwind { parse_cursor -> user_fn -> SourcePage::to_json_bytes }
}

pub extern "C" fn __stratum_estimated_count() -> i64 { -1 }
```

### Sink (`#[stratum_sink]`)

`__stratum_initialize` body is identical to source's but stores via
`__set_sink_config` (alongside the general `__set_plugin_config`). Three role
exports:

```rust
pub extern "C" fn __stratum_prepare(_ptr: u32, _len: u32) -> u32 { 0 }

pub extern "C" fn __stratum_finalize() -> u32 { 0 }

pub extern "C" fn __stratum_write_batch(ptr: u32, len: u32) -> u64 {
    // ... catch_unwind { PluginBatch::from_json_bytes -> user_fn -> WriteResult::to_json_bytes }
}
```

## Seeing the real expansion

If you'd like to see the *actual* compiler output (with full path resolution
and expanded macro paths), install `cargo-expand`:

```
cargo install cargo-expand
cargo expand --target wasm32-wasip1 --release
```

run from any plugin crate that uses one of the macros.
