use crate::abi::{pack_result_tail, shared_exports, InitBody};
use crate::common::{build_metadata_json, AttrArgs};
use proc_macro2::TokenStream;
use quote::quote;
use syn::ItemFn;

/// Expand `#[stratum_sink(...)]` into the full plugin module.
pub fn expand(attr: AttrArgs, user_fn: ItemFn) -> syn::Result<TokenStream> {
    let span = user_fn.sig.ident.span();
    let name = attr.require_name(span)?;
    let version = attr.require_version(span)?;

    // Sinks describe their *inputs* (the columns they expect to receive)
    // via `input`. `output` makes no sense - sinks return WriteResult -
    // and `output_schema` doesn't either, since sinks don't emit rows.
    if attr.output.is_some() {
        return Err(syn::Error::new(
            span,
            "`output` is not valid on #[stratum_sink]; sinks return WriteResult",
        ));
    }
    if !attr.output_schema.is_empty() {
        return Err(syn::Error::new(
            span,
            "sinks do not declare an output schema; use `input = [...]`",
        ));
    }

    let metadata = build_metadata_json(name, version, "sink", &attr.input_schema, &[], None);

    // `InitBody::Sink` makes init parse + stash config into `SINK_CONFIG`.
    let shared = shared_exports(&metadata, InitBody::Sink);
    let user_ident = &user_fn.sig.ident;
    let tail = pack_result_tail();

    // Optional lifecycle hooks.
    let lifecycle_body = |hook: &Option<String>| match hook {
        Some(name) => {
            let id = syn::Ident::new(name, span);
            quote! {
                let result = ::std::panic::catch_unwind(
                    || -> ::stratum_plugin_sdk::PluginResult<()> { #id() },
                );
                match result {
                    ::std::result::Result::Ok(::std::result::Result::Ok(())) => 0,
                    _ => 1,
                }
            }
        }
        None => quote! { 0 },
    };
    let prepare_body = lifecycle_body(&attr.prepare);
    let finalize_body = lifecycle_body(&attr.finalize);

    let role_entry = quote! {
        #[unsafe(no_mangle)]
        pub extern "C" fn __stratum_prepare(_ptr: u32, _len: u32) -> u32 {
            #prepare_body
        }

        #[unsafe(no_mangle)]
        pub extern "C" fn __stratum_finalize() -> u32 {
            #finalize_body
        }

        #[unsafe(no_mangle)]
        pub extern "C" fn __stratum_write_batch(ptr: u32, len: u32) -> u64 {
            let input_bytes = unsafe {
                ::stratum_plugin_sdk::runtime::abi::read_from_guest(ptr, len)
            };
            let result = ::std::panic::catch_unwind(
                || -> ::std::result::Result<::std::vec::Vec<u8>, ::stratum_plugin_sdk::PluginError> {
                    let batch = ::stratum_plugin_sdk::PluginBatch::from_json_bytes(&input_bytes)?;
                    let res: ::stratum_plugin_sdk::WriteResult = #user_ident(batch)?;
                    Ok(res.to_json_bytes())
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
