use crate::abi::{pack_result_tail, shared_exports, InitBody};
use crate::common::{build_metadata_json, AttrArgs};
use proc_macro2::TokenStream;
use quote::quote;
use syn::ItemFn;

/// Expand `#[stratum_transform(...)]` into the full plugin module.
pub fn expand(attr: AttrArgs, user_fn: ItemFn) -> syn::Result<TokenStream> {
    let span = user_fn.sig.ident.span();
    let name = attr.require_name(span)?;
    let version = attr.require_version(span)?;
    let output_type = attr
        .output
        .as_deref()
        .ok_or_else(|| syn::Error::new(span, "missing required attribute: output"))?;

    let metadata = build_metadata_json(
        name,
        version,
        "transform",
        &attr.input_schema,
        &[],
        Some(output_type),
    );

    let shared = shared_exports(&metadata, InitBody::None);
    let user_ident = &user_fn.sig.ident;
    let tail = pack_result_tail();

    let role_entry = quote! {
        #[unsafe(no_mangle)]
        pub extern "C" fn __stratum_transform(ptr: u32, len: u32) -> u64 {
            // Reclaim the bytes the host wrote into our linear memory.
            let input_bytes = unsafe {
                ::stratum_plugin_sdk::runtime::abi::read_from_guest(ptr, len)
            };
            let result = ::std::panic::catch_unwind(
                || -> ::std::result::Result<::std::vec::Vec<u8>, ::stratum_plugin_sdk::PluginError> {
                    let input = ::stratum_plugin_sdk::PluginInput::from_json_bytes(&input_bytes)?;
                    let value: ::stratum_plugin_sdk::Value = #user_ident(input)?.into();
                    Ok(::stratum_plugin_sdk::PluginOutput::new(value).to_json_bytes())
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
