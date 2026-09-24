extern crate proc_macro;

mod abi;
mod common;
mod filter;
mod sink;
mod source;
mod transform;

use crate::common::AttrArgs;
use proc_macro::TokenStream;
use syn::parse_macro_input;

/// Attribute macro for transform plugins.
///
/// Wraps a function of the shape `fn(PluginInput) -> PluginResult<T>` (where
/// `T: Into<Value>`) and emits the full transform ABI: shared exports plus
/// `__paganel_transform`.
///
/// ## Required attributes
///
/// - `name`    - plugin name (string).
/// - `version` - plugin version (string).
/// - `output`  - output type tag, e.g. `"f64"`, `"string"` (string).
/// - `input`   - schema array, e.g. `[{ name = "x", type = "f64", nullable = false }, ...]`.
#[proc_macro_attribute]
pub fn paganel_transform(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attr = parse_macro_input!(attr as AttrArgs);
    let user_fn = parse_macro_input!(item as syn::ItemFn);
    transform::expand(attr, user_fn)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Attribute macro for filter plugins.
///
/// Wraps a function of the shape `fn(PluginInput) -> PluginResult<FilterDecision>`
/// and emits the filter ABI: shared exports plus `__paganel_evaluate`.
///
/// ## Required attributes
///
/// - `name`, `version`, `input` (same shape as transform).
///
/// Filters do **not** accept `output` - they always return a `FilterDecision`.
#[proc_macro_attribute]
pub fn paganel_filter(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attr = parse_macro_input!(attr as AttrArgs);
    let user_fn = parse_macro_input!(item as syn::ItemFn);
    filter::expand(attr, user_fn)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Attribute macro for source plugins.
///
/// Wraps a function of the shape `fn(Option<String>) -> PluginResult<SourcePage>`
/// - the argument is the cursor handed by the host. Emits the source ABI:
/// shared exports plus `__paganel_read_page` and a default
/// `__paganel_estimated_count` that returns `-1` (unknown).
///
/// ## Required attributes
///
/// - `name`, `version`.
/// - `output_schema` - array of fields the source emits.
///
/// Sources do **not** accept `input` (they don't consume rows) or `output`.
/// The generated `__paganel_initialize` parses the config JSON the host
/// delivers and stashes it in the SDK's `OnceLock`; the user reads it via
/// `paganel_plugin_sdk::source_config()`.
#[proc_macro_attribute]
pub fn paganel_source(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attr = parse_macro_input!(attr as AttrArgs);
    let user_fn = parse_macro_input!(item as syn::ItemFn);
    source::expand(attr, user_fn)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Attribute macro for sink plugins.
///
/// Wraps a function of the shape `fn(PluginBatch) -> PluginResult<WriteResult>`
/// and emits the sink ABI: shared exports plus `__paganel_prepare`,
/// `__paganel_write_batch`, and `__paganel_finalize` (the prepare/finalize
/// pair are default no-op stubs in Phase 2).
///
/// ## Required attributes
///
/// - `name`, `version`, `input`.
///
/// Sinks do **not** accept `output` or `output_schema`. As with sources, the
/// generated init parses config into the SDK's `OnceLock`, readable via
/// `paganel_plugin_sdk::sink_config()`.
#[proc_macro_attribute]
pub fn paganel_sink(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attr = parse_macro_input!(attr as AttrArgs);
    let user_fn = parse_macro_input!(item as syn::ItemFn);
    sink::expand(attr, user_fn)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
