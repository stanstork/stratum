use proc_macro2::Span;
use serde_json::json;
use syn::ext::IdentExt;
use syn::parse::{Parse, ParseStream};
use syn::{braced, bracketed, Ident, LitBool, LitStr, Token};

/// One declared field in the input or output schema.
///
/// Mirrors the JSON shape the host expects:
/// `{ "name": "...", "type": "...", "nullable": bool }`.
#[derive(Clone)]
pub struct SchemaField {
    pub name: String,
    pub ty: String,
    pub nullable: bool,
}

/// All recognized keys from `#[stratum_*(...)]`.
#[derive(Default)]
pub struct AttrArgs {
    /// Plugin name. Required for every role.
    pub name: Option<String>,
    /// Plugin version. Required for every role.
    pub version: Option<String>,
    /// Output type tag (e.g. `"f64"`, `"string"`). Transform-only.
    pub output: Option<String>,
    /// Declared input columns. Used by transform / filter / sink.
    pub input_schema: Vec<SchemaField>,
    /// Declared output columns. Used by source.
    pub output_schema: Vec<SchemaField>,
    /// Name of the function to run before the first batch. Sink-only.
    pub prepare: Option<String>,
    /// Name of the function to run after the last batch. Sink-only.
    pub finalize: Option<String>,
}

impl Parse for AttrArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut args = AttrArgs::default();
        while !input.is_empty() {
            let key = Ident::parse_any(input)?;
            let _: Token![=] = input.parse()?;
            match key.to_string().as_str() {
                "name" => args.name = Some(parse_str(input)?),
                "version" => args.version = Some(parse_str(input)?),
                "output" => args.output = Some(parse_str(input)?),
                "input" | "input_schema" => {
                    args.input_schema = parse_schema_list(input)?;
                }
                "output_schema" => {
                    args.output_schema = parse_schema_list(input)?;
                }
                "prepare" => args.prepare = Some(parse_str(input)?),
                "finalize" => args.finalize = Some(parse_str(input)?),
                other => {
                    return Err(syn::Error::new(
                        key.span(),
                        format!("unknown attribute key: {}", other),
                    ));
                }
            }
            if input.is_empty() {
                break;
            }
            let _: Token![,] = input.parse()?;
        }
        Ok(args)
    }
}

impl AttrArgs {
    pub fn require_name(&self, span: Span) -> syn::Result<&str> {
        self.name
            .as_deref()
            .ok_or_else(|| syn::Error::new(span, "missing required attribute: name"))
    }

    pub fn require_version(&self, span: Span) -> syn::Result<&str> {
        self.version
            .as_deref()
            .ok_or_else(|| syn::Error::new(span, "missing required attribute: version"))
    }
}

fn parse_str(input: ParseStream) -> syn::Result<String> {
    let s: LitStr = input.parse()?;
    Ok(s.value())
}

fn parse_bool(input: ParseStream) -> syn::Result<bool> {
    let b: LitBool = input.parse()?;
    Ok(b.value)
}

/// Parses `[ { name = "a", type = "f64", nullable = false }, ... ]`.
///
/// `name` and `type` are required; `nullable` defaults to `false`.
fn parse_schema_list(input: ParseStream) -> syn::Result<Vec<SchemaField>> {
    let content;
    bracketed!(content in input);
    let mut out = Vec::new();
    while !content.is_empty() {
        let inner;
        braced!(inner in content);
        let mut name: Option<String> = None;
        let mut ty: Option<String> = None;
        let mut nullable = false;
        while !inner.is_empty() {
            // `type` is a Rust keyword, so the plain `Ident` parser would
            // reject it. `parse_any` lets us accept keywords as raw idents.
            let key = Ident::parse_any(&inner)?;
            let _: Token![=] = inner.parse()?;
            match key.to_string().as_str() {
                "name" => name = Some(parse_str(&inner)?),
                "type" => ty = Some(parse_str(&inner)?),
                "nullable" => nullable = parse_bool(&inner)?,
                other => {
                    return Err(syn::Error::new(
                        key.span(),
                        format!("unknown schema field key: {}", other),
                    ));
                }
            }
            if inner.is_empty() {
                break;
            }
            let _: Token![,] = inner.parse()?;
        }
        let name =
            name.ok_or_else(|| syn::Error::new(content.span(), "schema entry missing 'name'"))?;
        let ty =
            ty.ok_or_else(|| syn::Error::new(content.span(), "schema entry missing 'type'"))?;
        out.push(SchemaField { name, ty, nullable });
        if content.is_empty() {
            break;
        }
        let _: Token![,] = content.parse()?;
    }
    Ok(out)
}

/// Build the `__stratum_metadata` JSON document as a stable string.
pub fn build_metadata_json(
    name: &str,
    version: &str,
    plugin_type: &str,
    input_schema: &[SchemaField],
    output_schema: &[SchemaField],
    output_type: Option<&str>,
) -> String {
    let mut value = serde_json::Map::new();
    value.insert("name".into(), json!(name));
    value.insert("version".into(), json!(version));
    value.insert("type".into(), json!(plugin_type));
    value.insert("exchange_format".into(), json!("json_v1"));
    value.insert("runtime".into(), json!("native"));

    if !input_schema.is_empty() {
        let arr: Vec<_> = input_schema
            .iter()
            .map(|f| {
                json!({
                    "name": f.name,
                    "type": f.ty,
                    "nullable": f.nullable,
                })
            })
            .collect();
        value.insert("input_schema".into(), json!(arr));
    }
    if !output_schema.is_empty() {
        let arr: Vec<_> = output_schema
            .iter()
            .map(|f| {
                json!({
                    "name": f.name,
                    "type": f.ty,
                    "nullable": f.nullable,
                })
            })
            .collect();
        value.insert("output_schema".into(), json!(arr));
    }
    if let Some(t) = output_type {
        value.insert("output_type".into(), json!(t));
    }

    serde_json::to_string(&serde_json::Value::Object(value))
        .expect("metadata JSON serialization is infallible")
}
