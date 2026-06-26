use crate::abi::{pack_result_tail, shared_exports, InitBody};
use crate::common::{build_metadata_json, AttrArgs};
use proc_macro2::TokenStream;
use quote::quote;
use syn::ItemFn;

/// Expand `#[stratum_source(...)]` into the full plugin module.
pub fn expand(attr: AttrArgs, user_fn: ItemFn) -> syn::Result<TokenStream> {
    let span = user_fn.sig.ident.span();
    let name = attr.require_name(span)?;
    let version = attr.require_version(span)?;

    // Sources describe their *outputs* via `output_schema`. `output` (the
    // single-value form used by transforms) is meaningless here, and an
    // `input` schema is wrong because sources don't consume rows.
    if attr.output.is_some() {
        return Err(syn::Error::new(
            span,
            "`output` is not valid on #[stratum_source]; use `output_schema = [...]` instead",
        ));
    }
    if !attr.input_schema.is_empty() {
        return Err(syn::Error::new(
            span,
            "sources do not declare an input schema; use `output_schema = [...]`",
        ));
    }

    let metadata = build_metadata_json(name, version, "source", &[], &attr.output_schema, None);

    // `InitBody::Source` makes the generated init parse the config JSON
    // and store it in `SOURCE_CONFIG`. The user's read function fetches
    // it via `stratum_plugin_sdk::source_config()`.
    let shared = shared_exports(&metadata, InitBody::Source);
    let user_ident = &user_fn.sig.ident;
    let tail = pack_result_tail();

    let role_entry = quote! {
        #[unsafe(no_mangle)]
        pub extern "C" fn __stratum_read_page(ptr: u32, len: u32) -> u64 {
            let input_bytes = unsafe {
                ::stratum_plugin_sdk::runtime::abi::read_from_guest(ptr, len)
            };
            let result = ::std::panic::catch_unwind(
                || -> ::std::result::Result<::std::vec::Vec<u8>, ::stratum_plugin_sdk::PluginError> {
                    let cursor = ::stratum_plugin_sdk::runtime::parse_cursor(&input_bytes)?;
                    let page: ::stratum_plugin_sdk::SourcePage = #user_ident(cursor)?;
                    Ok(page.to_json_bytes())
                },
            );
            #tail
        }

        #[unsafe(no_mangle)]
        pub extern "C" fn __stratum_estimated_count() -> i64 {
            -1
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
