use crate::abi::{pack_result_tail, shared_exports, InitBody};
use crate::common::{build_metadata_json, AttrArgs};
use proc_macro2::TokenStream;
use quote::quote;
use syn::ItemFn;

/// Expand `#[paganel_filter(...)]` into the full plugin module.
pub fn expand(attr: AttrArgs, user_fn: ItemFn) -> syn::Result<TokenStream> {
    let span = user_fn.sig.ident.span();
    let name = attr.require_name(span)?;
    let version = attr.require_version(span)?;

    // Filters don't have a configurable output type - they always return
    // FilterDecision. Reject `output` explicitly so users get a clear
    // message instead of silently-ignored configuration.
    if attr.output.is_some() {
        return Err(syn::Error::new(
            span,
            "`output` is not valid on #[paganel_filter]; filters return FilterDecision",
        ));
    }

    let metadata = build_metadata_json(name, version, "filter", &attr.input_schema, &[], None);

    let shared = shared_exports(&metadata, InitBody::None);
    let user_ident = &user_fn.sig.ident;
    let tail = pack_result_tail();

    // Batch-native ABI: the user's function takes `Vec<PluginInput>` and returns one
    // `FilterDecision` per input, owning the iteration. No per-row entry point.
    let role_entry = quote! {
        #[unsafe(no_mangle)]
        pub extern "C" fn __paganel_evaluate(ptr: u32, len: u32) -> u64 {
            let input_bytes = unsafe {
                ::paganel_plugin_sdk::runtime::abi::read_from_guest(ptr, len)
            };
            let result = ::std::panic::catch_unwind(
                || -> ::std::result::Result<::std::vec::Vec<u8>, ::paganel_plugin_sdk::PluginError> {
                    let inputs = ::paganel_plugin_sdk::columnar::decode_input_batch(&input_bytes)?;
                    let decisions: ::std::vec::Vec<::paganel_plugin_sdk::FilterDecision> =
                        #user_ident(inputs)?;
                    Ok(::paganel_plugin_sdk::columnar::encode_filter_batch(&decisions))
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
