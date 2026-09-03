use super::shared::{json_to_value, limits_for, read_input, read_text};
use crate::{compile::resolve_plugin_wasm, error::CliError};
use engine_wasm::{
    exchange::types::PluginInput,
    runtime::{
        engine::{WasmEngine, WasmEngineConfig},
        limits::HostCapabilities,
    },
};
use model::{
    core::{types::Type, value::FieldValue},
    records::{OpType, Record},
};
use std::path::Path;

/// `pag plugin test <plugin.wasm> [--mode ...] [--input ...]` - run a plugin
/// once with sample input and print the result.
pub fn run(
    path: &Path,
    mode: Option<&str>,
    input: Option<&str>,
    cursor: Option<&str>,
    config_json: Option<&str>,
    as_json: bool,
) -> Result<(), CliError> {
    if !path.exists() {
        return Err(CliError::UserMessage(format!(
            "no such plugin: {}",
            path.display()
        )));
    }

    let wasm = resolve_plugin_wasm(path)?; // compile .js if needed
    let mut engine = WasmEngine::new(WasmEngineConfig::default())?;
    let module = engine.load_module(&wasm)?;
    let meta = engine.read_metadata(&module)?;

    // Limits sized off the runtime hint (no PPL decl here).
    let limits = limits_for(&meta, None);
    let config = config_json
        .map(|p| read_text(Some(p)))
        .transpose()?
        .map(String::into_bytes);

    let mut inst = engine.instantiate(
        &module,
        "<test>".to_string(),
        HostCapabilities::default(),
        limits,
        config.as_deref(),
    )?;

    let role = mode
        .map(str::to_lowercase)
        .unwrap_or_else(|| format!("{:?}", meta.plugin_type).to_lowercase());

    match role.as_str() {
        "transform" => {
            let values: Vec<String> = inst
                .call_transform(&build_inputs(input)?)?
                .iter()
                .map(|o| format!("{:?}", o.value))
                .collect();

            emit(
                as_json,
                &values.join("\n"),
                || serde_json::json!({ "values": values }),
            );
        }
        "filter" => {
            let passes: Vec<bool> = inst
                .call_evaluate(&build_inputs(input)?)?
                .iter()
                .map(|d| d.is_pass())
                .collect();

            let human = passes
                .iter()
                .map(|&p| if p { "PASS" } else { "REJECT" })
                .collect::<Vec<_>>()
                .join("\n");

            emit(as_json, &human, || serde_json::json!({ "passes": passes }));
        }
        "source" => {
            let page = inst.call_read_page(cursor, 100)?;
            let (n, more, next) = (page.records.len(), page.has_more, page.next_cursor.clone());
            emit(
                as_json,
                &format!("{n} rows, has_more={more}, next_cursor={next:?}"),
                || serde_json::json!({ "rows": n, "has_more": more, "next_cursor": next }),
            );
        }
        "sink" => {
            let rows = build_records(input)?;
            inst.call_prepare()?; // no-op if the plugin has no prepare hook

            let res = inst.call_write_batch(&rows)?;
            inst.call_finalize()?;

            emit(
                as_json,
                &format!("rows_written={}", res.rows_written),
                || serde_json::json!({ "rows_written": res.rows_written }),
            );
        }
        other => {
            return Err(CliError::UserMessage(format!(
                "unknown --mode '{other}' (expected transform|filter|source|sink)"
            )));
        }
    }
    Ok(())
}

fn emit(as_json: bool, human: &str, json: impl FnOnce() -> serde_json::Value) {
    if as_json {
        println!(
            "{}",
            serde_json::to_string_pretty(&json()).unwrap_or_default()
        );
    } else {
        println!("{human}");
    }
}

/// Build a batch of `PluginInput`s from the test input.
fn build_inputs(src: Option<&str>) -> Result<Vec<PluginInput>, CliError> {
    let rows = parse_input_rows(src, false)?;

    if rows.is_empty() {
        return Ok(vec![PluginInput::new()]);
    }

    Ok(rows
        .into_iter()
        .map(|obj| {
            let mut pin = PluginInput::new();
            for (k, v) in obj {
                pin.insert(k, json_to_value(&v));
            }
            pin
        })
        .collect())
}

/// Build a batch of `Record`s from a JSON array of rows or `{ "records": [...] }`.
fn build_records(src: Option<&str>) -> Result<Vec<Record>, CliError> {
    let rows = parse_input_rows(src, true)?;

    Ok(rows
        .into_iter()
        .map(|obj| {
            let fields = obj
                .into_iter()
                .map(|(name, v)| FieldValue {
                    name,
                    value: Some(json_to_value(&v)),
                    // Placeholder: the wire layer only serializes name + value.
                    data_type: Type::Unknown {
                        source_name: String::new(),
                        fallback_ddl: String::new(),
                    },
                })
                .collect();

            Record::from_fields("<test>", fields, OpType::Insert)
        })
        .collect())
}

fn parse_input_rows(
    src: Option<&str>,
    allow_records_wrapper: bool,
) -> Result<Vec<serde_json::Map<String, serde_json::Value>>, CliError> {
    let text = read_input(src)?;
    if text.trim().is_empty() {
        return Ok(Vec::new());
    }

    let json: serde_json::Value = serde_json::from_str(text.trim())
        .map_err(|e| CliError::UserMessage(format!("invalid input JSON: {e}")))?;

    let rows = match json {
        serde_json::Value::Array(a) => a,
        serde_json::Value::Object(mut o) => {
            if allow_records_wrapper && o.contains_key("records") {
                let records = o.remove("records").unwrap();
                if let serde_json::Value::Array(a) = records {
                    a
                } else {
                    return Err(CliError::UserMessage(
                        "expected 'records' to be a JSON array".into(),
                    ));
                }
            } else {
                // Treat a single unwrapped object as a batch of 1
                vec![serde_json::Value::Object(o)]
            }
        }
        _ => {
            return Err(CliError::UserMessage(
                "input must be a JSON array of row objects (or a single object)".into(),
            ));
        }
    };

    rows.into_iter()
        .map(|row| match row {
            serde_json::Value::Object(obj) => Ok(obj),
            _ => Err(CliError::UserMessage(
                "each input row must be a JSON object".into(),
            )),
        })
        .collect()
}
