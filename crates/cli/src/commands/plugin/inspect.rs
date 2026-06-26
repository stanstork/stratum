use crate::{compile::resolve_plugin_wasm, error::CliError};
use engine_wasm::{
    runtime::engine::{WasmEngine, WasmEngineConfig},
    schema::PluginType,
};
use std::path::Path;

pub fn run(path: &Path, as_json: bool) -> Result<(), CliError> {
    if !path.exists() {
        return Err(CliError::UserMessage(format!(
            "no such plugin: {}",
            path.display()
        )));
    }

    let wasm = resolve_plugin_wasm(path)?; // compile .js if needed
    let mut engine = WasmEngine::new(WasmEngineConfig::default())?;
    let module = engine.load_module(&wasm)?;
    let meta = engine.read_metadata(&module)?; // metadata-only - no init/QuickJS

    if as_json {
        println!("{}", serde_json::to_string_pretty(&meta)?);
        return Ok(());
    }

    println!("Plugin:  {}", meta.name);
    println!("Version: {}", meta.version);
    println!("Role:    {:?}", meta.plugin_type);
    println!("Wire:    {:?}", meta.exchange_format);
    match meta.plugin_type {
        PluginType::Transform => {
            print_fields("Input", &meta.input_schema);
            println!("Output:  {}", meta.output_type.as_deref().unwrap_or("?"));
        }
        PluginType::Filter => print_fields("Input", &meta.input_schema),
        PluginType::Source => print_fields("Output", &meta.output_schema),
        PluginType::Sink => print_fields("Input", &meta.input_schema),
    }
    Ok(())
}

fn print_fields(label: &str, fields: &[engine_wasm::schema::PluginField]) {
    println!("{label}:");
    for f in fields {
        let null = if f.nullable { " (nullable)" } else { "" };
        println!("  - {}: {}{}", f.name, f.field_type, null);
    }
}
