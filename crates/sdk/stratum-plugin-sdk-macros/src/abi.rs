use proc_macro2::TokenStream;
use quote::quote;

/// What the generated `__stratum_initialize` should do beyond installing
/// the panic hook.
///
/// Every variant parses the host config blob into the general config store
/// (`__set_plugin_config`, readable via `stratum_plugin_sdk::config()`).
/// Source/sink additionally stash a role-specific config.
///
/// - [`InitBody::None`] - transform / filter. Install the hook and load the
///   general `config()` only.
/// - [`InitBody::Source`] / [`InitBody::Sink`] - same, plus stash a
///   role-specific config in the SDK's `OnceLock` so the user's handler can
///   read it via `stratum_plugin_sdk::source_config()` / `sink_config()`.
pub enum InitBody {
    /// Transform / filter - general `config()` only, no role-specific config.
    None,
    /// Source plugin - also store the parsed config in `SOURCE_CONFIG`.
    Source,
    /// Sink plugin - also store the parsed config in `SINK_CONFIG`.
    Sink,
}

/// Emits the per-plugin shared exports common to every role.
pub fn shared_exports(metadata_json: &str, init: InitBody) -> TokenStream {
    let init_body = match init {
        InitBody::None => quote! {
            ::stratum_plugin_sdk::runtime::panic::install_panic_hook();
            let bytes = unsafe { ::stratum_plugin_sdk::runtime::abi::read_from_guest(ptr, len) };
            match ::stratum_plugin_sdk::runtime::parse_config(&bytes) {
                Ok(params) => {
                    ::stratum_plugin_sdk::__set_plugin_config(
                        ::stratum_plugin_sdk::PluginConfig::new(params),
                    );
                    0
                }
                // Returning non-zero tells the host initialization failed.
                // The host treats the plugin as unusable from this point on.
                Err(_) => 1,
            }
        },
        InitBody::Source => quote! {
            ::stratum_plugin_sdk::runtime::panic::install_panic_hook();
            let bytes = unsafe { ::stratum_plugin_sdk::runtime::abi::read_from_guest(ptr, len) };
            match ::stratum_plugin_sdk::runtime::parse_config(&bytes) {
                Ok(params) => {
                    ::stratum_plugin_sdk::__set_plugin_config(
                        ::stratum_plugin_sdk::PluginConfig::new(params.clone()),
                    );
                    ::stratum_plugin_sdk::__set_source_config(
                        ::stratum_plugin_sdk::SourceConfig::new(params),
                    );
                    0
                }
                Err(_) => 1,
            }
        },
        InitBody::Sink => quote! {
            ::stratum_plugin_sdk::runtime::panic::install_panic_hook();
            let bytes = unsafe { ::stratum_plugin_sdk::runtime::abi::read_from_guest(ptr, len) };
            match ::stratum_plugin_sdk::runtime::parse_config(&bytes) {
                Ok(params) => {
                    ::stratum_plugin_sdk::__set_plugin_config(
                        ::stratum_plugin_sdk::PluginConfig::new(params.clone()),
                    );
                    ::stratum_plugin_sdk::__set_sink_config(
                        ::stratum_plugin_sdk::SinkConfig::new(params),
                    );
                    0
                }
                Err(_) => 1,
            }
        },
    };

    quote! {
        // Sentinel. Defining the same `no_mangle` symbol twice is a linker
        // error, so attaching two role macros in the same crate fails to build.
        #[doc(hidden)]
        #[unsafe(no_mangle)]
        pub static __STRATUM_PLUGIN_SENTINEL: u8 = 0;

        // Host-callable allocator: gives the host a chunk inside our
        // linear memory it can fill with bytes for the next call.
        #[unsafe(no_mangle)]
        pub extern "C" fn __stratum_alloc(size: u32) -> u32 {
            unsafe { ::stratum_plugin_sdk::runtime::abi::alloc_bytes(size) }
        }

        // Host-callable deallocator: frees a slot previously handed out by `__stratum_alloc`.
        #[unsafe(no_mangle)]
        pub extern "C" fn __stratum_dealloc(ptr: u32, size: u32) {
            unsafe { ::stratum_plugin_sdk::runtime::abi::dealloc_bytes(ptr, size); }
        }

        // The metadata JSON is baked in at compile time as a static &str -
        // there is no runtime cost to producing it.
        static __STRATUM_METADATA_JSON: &str = #metadata_json;

        // Returns the metadata JSON to the host as a packed (ptr, len).
        #[unsafe(no_mangle)]
        pub extern "C" fn __stratum_metadata() -> u64 {
            let (p, l) = unsafe {
                ::stratum_plugin_sdk::runtime::abi::write_to_guest(__STRATUM_METADATA_JSON.as_bytes())
            };
            ::stratum_plugin_sdk::runtime::pack::pack(p, l)
        }

        // The init hook.
        #[unsafe(no_mangle)]
        pub extern "C" fn __stratum_initialize(ptr: u32, len: u32) -> u32 {
            let _ = (ptr, len);
            #init_body
        }

        // Reserved for graceful teardown.
        #[unsafe(no_mangle)]
        pub extern "C" fn __stratum_shutdown() {}
    }
}

/// The shared tail that every role's entry point uses to convert a
/// `catch_unwind` result into the packed `u64` the host expects.
pub fn pack_result_tail() -> TokenStream {
    quote! {
        let bytes = match result {
            Ok(Ok(b)) => b,
            Ok(Err(e)) => ::stratum_plugin_sdk::error::serialize_error(&e),
            Err(_) => {
                // The panic hook stored the formatted message; fall back
                // to a generic string if it somehow wasn't captured.
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
}
