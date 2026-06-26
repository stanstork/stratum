use crate::abi::{pack_result_tail, shared_exports, InitBody};
use crate::common::{build_metadata_json, AttrArgs};
use proc_macro2::TokenStream;
use quote::quote;
use syn::ItemFn;

/// Expand `#[stratum_filter(...)]` into the full plugin module.
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
            "`output` is not valid on #[stratum_filter]; filters return FilterDecision",
        ));
    }

    let metadata = build_metadata_json(name, version, "filter", &attr.input_schema, &[], None);

    let shared = shared_exports(&metadata, InitBody::None);
    let user_ident = &user_fn.sig.ident;
    let tail = pack_result_tail();

    let role_entry = quote! {
        #[unsafe(no_mangle)]
        pub extern "C" fn __stratum_evaluate(ptr: u32, len: u32) -> u64 {
            let input_bytes = unsafe {
                ::stratum_plugin_sdk::runtime::abi::read_from_guest(ptr, len)
            };
            let result = ::std::panic::catch_unwind(
                || -> ::std::result::Result<::std::vec::Vec<u8>, ::stratum_plugin_sdk::PluginError> {
                    let input = ::stratum_plugin_sdk::PluginInput::from_json_bytes(&input_bytes)?;
                    let decision: ::stratum_plugin_sdk::FilterDecision = #user_ident(input)?;
                    Ok(decision.to_json_bytes())
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
