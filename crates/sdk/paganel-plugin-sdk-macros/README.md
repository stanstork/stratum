# paganel-plugin-sdk-macros

Attribute macros behind [`paganel-plugin-sdk`](https://crates.io/crates/paganel-plugin-sdk):
`#[paganel_transform]`, `#[paganel_filter]`, `#[paganel_source]` and
`#[paganel_sink]`.

Each one wraps a plain Rust function and emits the full WASM ABI a
[Paganel](https://github.com/stanstork/paganel) plugin needs: the metadata
export the host reads, allocation and lifecycle exports, batch decoding, panic
isolation, and the role entry point itself.

You don't need to depend on this crate directly. `paganel-plugin-sdk`
re-exports every macro; depend on that instead.

See [macro expansion](https://github.com/stanstork/paganel/blob/main/docs/plugins/macro-expansion.md)
for what the generated code looks like.

## License

MIT.
