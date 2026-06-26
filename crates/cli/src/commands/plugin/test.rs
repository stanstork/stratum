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

/// `stratum plugin test <plugin.wasm> [--mode ...] [--input ...]` - run a plugin
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

    // Limits sized off the runtime hint (no SMQL decl here).
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
            let out = inst.call_transform(&build_input(input)?)?;
            emit(
                as_json,
                &format!("{:?}", out.value),
                || serde_json::json!({ "value": format!("{:?}", out.value) }),
            );
        }
        "filter" => {
            let d = inst.call_evaluate(&build_input(input)?)?;
            let pass = d.is_pass();
            emit(
                as_json,
                if pass { "PASS" } else { "REJECT" },
                || serde_json::json!({ "pass": pass }),
            );
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

/// Build a `PluginInput` from a JSON object of plain scalars.
fn build_input(src: Option<&str>) -> Result<PluginInput, CliError> {
    let text = read_input(src)?;
    if text.trim().is_empty() {
        return Ok(PluginInput::new());
    }
    let json: serde_json::Value = serde_json::from_str(text.trim())
        .map_err(|e| CliError::UserMessage(format!("invalid input JSON: {e}")))?;
    let obj = json
        .as_object()
        .ok_or_else(|| CliError::UserMessage("input must be a JSON object".into()))?;
    let mut pin = PluginInput::new();
    for (k, v) in obj {
        pin.insert(k.clone(), json_to_value(v));
    }
    Ok(pin)
}

/// Build a batch of `Record`s from a JSON array of rows or `{ "records": [...] }`.
fn build_records(src: Option<&str>) -> Result<Vec<Record>, CliError> {
    let text = read_input(src)?;
    if text.trim().is_empty() {
        return Ok(Vec::new());
    }

    let json: serde_json::Value = serde_json::from_str(text.trim())
        .map_err(|e| CliError::UserMessage(format!("invalid input JSON: {e}")))?;
    let rows = match &json {
        serde_json::Value::Array(a) => a.clone(),
        serde_json::Value::Object(o) => o
            .get("records")
            .and_then(|r| r.as_array())
            .cloned()
            .ok_or_else(|| {
                CliError::UserMessage("expected a JSON array of rows or {\"records\":[...]}".into())
            })?,
        _ => {
            return Err(CliError::UserMessage(
                "expected a JSON array of rows".into(),
            ));
        }
    };

    let mut records = Vec::with_capacity(rows.len());
    for row in rows {
        let obj = row
            .as_object()
            .ok_or_else(|| CliError::UserMessage("each record must be a JSON object".into()))?;
        let fields = obj
            .iter()
            .map(|(k, v)| FieldValue {
                name: k.clone(),
                value: Some(json_to_value(v)),
                // Placeholder: the wire layer only serializes name + value.
                data_type: Type::Unknown {
                    source_name: String::new(),
                    fallback_ddl: String::new(),
                },
            })
            .collect();
        records.push(Record::new("<test>", fields, OpType::Insert));
    }
    Ok(records)
}
