use crate::plan::diagnostics::diagnostic::Diagnostic;
use engine_wasm::{
    schema::PluginMetadata,
    validation::{Compat, check},
};
use model::{
    core::types::Type,
    execution::pipeline::{PluginTransformCall, ValidationRule},
};
use std::collections::HashMap;

/// Validate a transform plugin call (`select { col = plugin.x({...}) }`).
pub fn validate_transform_call(
    pipeline: &str,
    call: &PluginTransformCall,
    available: &HashMap<String, Type>,
    plugin: &PluginMetadata,
    dest_col_type: Option<&Type>,
) -> Vec<Diagnostic> {
    let mut out = Vec::new();

    if plugin.input_schema.is_empty() {
        out.push(
            Diagnostic::info(
                "PLUGIN_INPUT_SCHEMA_EMPTY",
                &format!(
                    "plugin '{}' declares no input schema; skipping input type checks",
                    call.plugin_name
                ),
            )
            .with_pipeline(pipeline),
        );
    }

    // Each declared input field must be supplied and type-compatible.
    for field in &plugin.input_schema {
        // input_mapping maps plugin_field -> source_column
        let Some(src_col) = call.input_mapping.get(&field.name) else {
            out.push(
                Diagnostic::error(
                    "PLUGIN_INPUT_UNMAPPED",
                    &format!(
                        "plugin '{}' expects input field '{}' but the call maps nothing to it",
                        call.plugin_name, field.name
                    ),
                )
                .with_pipeline(pipeline),
            );
            continue;
        };
        let Some(src_ty) = available.get(src_col) else {
            out.push(
                Diagnostic::error(
                    "PLUGIN_INPUT_MISSING_COLUMN",
                    &format!(
                        "input field '{}' maps to column '{}', which is not available",
                        field.name, src_col
                    ),
                )
                .with_pipeline(pipeline),
            );
            continue;
        };
        match check(src_ty, &field.to_canonical_type()) {
            Compat::Ok => {}
            Compat::Lossy(note) => out.push(
                Diagnostic::warning(
                    "PLUGIN_INPUT_TYPE_LOSSY",
                    &format!(
                        "field '{}': {} (source {:?} -> plugin {})",
                        field.name, note, src_ty, field.field_type
                    ),
                )
                .with_pipeline(pipeline),
            ),
            Compat::Incompatible => out.push(
                Diagnostic::error(
                    "PLUGIN_INPUT_TYPE_MISMATCH",
                    &format!(
                        "field '{}': plugin expects {}, source column '{}' is {:?}",
                        field.name, field.field_type, src_col, src_ty
                    ),
                )
                .with_pipeline(pipeline),
            ),
        }
    }

    // Output type vs destination column type (transforms only).
    if let (Some(out_ty), Some(dest_ty)) = (plugin.canonical_output_type(), dest_col_type) {
        match check(&out_ty, dest_ty) {
            Compat::Ok => {}
            Compat::Lossy(note) => out.push(
                Diagnostic::warning(
                    "PLUGIN_OUTPUT_TYPE_LOSSY",
                    &format!(
                        "plugin '{}' outputs {:?}, destination column '{}' is {:?}: {}",
                        call.plugin_name, out_ty, call.output_column, dest_ty, note
                    ),
                )
                .with_pipeline(pipeline),
            ),
            Compat::Incompatible => out.push(
                Diagnostic::error(
                    "PLUGIN_OUTPUT_TYPE_MISMATCH",
                    &format!(
                        "plugin '{}' outputs {:?}, destination column '{}' is {:?}",
                        call.plugin_name, out_ty, call.output_column, dest_ty
                    ),
                )
                .with_pipeline(pipeline),
            ),
        }
    }

    // Output column shadows a source column of a different type.
    if let (Some(out_ty), Some(src_ty)) = (
        plugin.canonical_output_type(),
        available.get(&call.output_column),
    ) && &out_ty != src_ty
    {
        out.push(
            Diagnostic::warning(
                "PLUGIN_OUTPUT_SHADOWS_SOURCE",
                &format!(
                    "plugin '{}' output column '{}' shadows source column of type {:?}; \
                     source values are discarded and the column is created as {:?}",
                    call.plugin_name, call.output_column, src_ty, out_ty
                ),
            )
            .with_pipeline(pipeline),
        );
    }

    out
}

/// Validate a `validate { rule "..." { filter = plugin.x({...}) … } }` rule.
pub fn validate_filter_rule(
    pipeline: &str,
    rule: &ValidationRule,
    plugin_name: &str,
    input_mapping: &HashMap<String, String>,
    available: &HashMap<String, Type>,
    plugin: &PluginMetadata,
) -> Vec<Diagnostic> {
    let mut out = Vec::new();

    if plugin.input_schema.is_empty() {
        out.push(
            Diagnostic::info(
                "PLUGIN_INPUT_SCHEMA_EMPTY",
                &format!(
                    "filter plugin '{}' (rule '{}') declares no input schema; skipping input type checks",
                    plugin_name, rule.label
                ),
            )
            .with_pipeline(pipeline),
        );
    }

    for field in &plugin.input_schema {
        let Some(src_col) = input_mapping.get(&field.name) else {
            out.push(
                Diagnostic::error(
                    "PLUGIN_INPUT_UNMAPPED",
                    &format!(
                        "filter rule '{}': plugin '{}' expects input field '{}' but the rule maps nothing to it",
                        rule.label, plugin_name, field.name
                    ),
                )
                .with_pipeline(pipeline),
            );
            continue;
        };
        let Some(src_ty) = available.get(src_col) else {
            out.push(
                Diagnostic::error(
                    "PLUGIN_INPUT_MISSING_COLUMN",
                    &format!(
                        "filter rule '{}': input field '{}' maps to column '{}', which is not available",
                        rule.label, field.name, src_col
                    ),
                )
                .with_pipeline(pipeline),
            );
            continue;
        };
        match check(src_ty, &field.to_canonical_type()) {
            Compat::Ok => {}
            Compat::Lossy(note) => out.push(
                Diagnostic::warning(
                    "PLUGIN_INPUT_TYPE_LOSSY",
                    &format!(
                        "filter rule '{}', field '{}': {} (column '{}' is {:?}, plugin expects {})",
                        rule.label, field.name, note, src_col, src_ty, field.field_type
                    ),
                )
                .with_pipeline(pipeline),
            ),
            Compat::Incompatible => out.push(
                Diagnostic::error(
                    "PLUGIN_INPUT_TYPE_MISMATCH",
                    &format!(
                        "filter rule '{}', field '{}': plugin expects {}, column '{}' is {:?}",
                        rule.label, field.name, field.field_type, src_col, src_ty
                    ),
                )
                .with_pipeline(pipeline),
            ),
        }
    }
    out
}

/// Validate a source plugin endpoint: every column referenced in the select
/// block must exist in the plugin's declared `output_schema`.
pub fn validate_source_endpoint(
    pipeline: &str,
    plugin_name: &str,
    referenced_columns: &[String],
    plugin: &PluginMetadata,
) -> Vec<Diagnostic> {
    if plugin.output_schema.is_empty() {
        return vec![
            Diagnostic::info(
                "PLUGIN_OUTPUT_SCHEMA_EMPTY",
                &format!(
                    "source plugin '{}' declares no output schema; skipping column reference checks",
                    plugin_name
                ),
            )
            .with_pipeline(pipeline),
        ];
    }
    let available: Vec<&str> = plugin
        .output_schema
        .iter()
        .map(|f| f.name.as_str())
        .collect();
    referenced_columns
        .iter()
        // Select references are qualified by the source alias (e.g. `counter.id`)
        // while the plugin declares bare field names; compare the final segment.
        .filter(|c| {
            let bare = c.as_str().rsplit('.').next().unwrap_or(c.as_str());
            !available.contains(&bare)
        })
        .map(|c| {
            Diagnostic::error(
                "PLUGIN_SOURCE_COLUMN_MISSING",
                &format!(
                    "column '{}' referenced in select but not in source plugin '{}' output_schema",
                    c, plugin_name
                ),
            )
            .with_pipeline(pipeline)
            .with_suggestion(&format!("available columns: {}", available.join(", ")))
        })
        .collect()
}

/// Validate a sink plugin endpoint: every column the plugin declares as input
/// must be produced by the pipeline, with a compatible type.
pub fn validate_sink_endpoint(
    pipeline: &str,
    plugin_name: &str,
    produced: &HashMap<String, Type>,
    plugin: &PluginMetadata,
) -> Vec<Diagnostic> {
    if plugin.input_schema.is_empty() {
        return vec![
            Diagnostic::info(
                "PLUGIN_INPUT_SCHEMA_EMPTY",
                &format!(
                    "sink plugin '{}' declares no input schema; skipping column type checks",
                    plugin_name
                ),
            )
            .with_pipeline(pipeline),
        ];
    }

    let mut out = Vec::new();
    for field in &plugin.input_schema {
        let Some(src_ty) = produced.get(&field.name) else {
            out.push(
                Diagnostic::error(
                    "PLUGIN_SINK_COLUMN_MISSING",
                    &format!(
                        "sink plugin '{}' expects column '{}' but the pipeline does not produce it",
                        plugin_name, field.name
                    ),
                )
                .with_pipeline(pipeline),
            );
            continue;
        };
        match check(src_ty, &field.to_canonical_type()) {
            Compat::Ok => {}
            Compat::Lossy(note) => out.push(
                Diagnostic::warning(
                    "PLUGIN_SINK_TYPE_LOSSY",
                    &format!(
                        "sink column '{}': {} (pipeline produces {:?}, plugin expects {})",
                        field.name, note, src_ty, field.field_type
                    ),
                )
                .with_pipeline(pipeline),
            ),
            Compat::Incompatible => out.push(
                Diagnostic::error(
                    "PLUGIN_SINK_TYPE_MISMATCH",
                    &format!(
                        "sink column '{}': plugin expects {}, pipeline produces {:?}",
                        field.name, field.field_type, src_ty
                    ),
                )
                .with_pipeline(pipeline),
            ),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::diagnostics::level::DiagnosticLevel;
    use engine_wasm::{
        exchange::ExchangeFormat,
        schema::{PluginField, PluginRuntime, PluginType},
    };
    use model::core::types::{FloatSize, IntSize};

    fn i64_ty() -> Type {
        Type::Int {
            bits: IntSize::I64,
            unsigned: false,
            auto_increment: false,
        }
    }
    fn f64_ty() -> Type {
        Type::Float {
            bits: FloatSize::F64,
        }
    }

    fn transform_meta() -> PluginMetadata {
        PluginMetadata {
            name: "score_risk".into(),
            version: "1.0".into(),
            plugin_type: PluginType::Transform,
            exchange_format: ExchangeFormat::JsonV1,
            input_schema: vec![PluginField {
                name: "amount".into(),
                field_type: "i64".into(),
                nullable: false,
            }],
            output_schema: vec![],
            output_type: Some("f64".into()),
            runtime: PluginRuntime::Native,
        }
    }

    fn call(plugin: &str, output: &str, mapping: &[(&str, &str)]) -> PluginTransformCall {
        PluginTransformCall {
            plugin_name: plugin.into(),
            output_column: output.into(),
            input_mapping: mapping
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    fn map(entries: &[(&str, Type)]) -> HashMap<String, Type> {
        entries
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    fn codes(diags: &[Diagnostic]) -> Vec<&str> {
        diags.iter().map(|d| d.code.as_str()).collect()
    }

    #[test]
    fn transform_happy_path_emits_nothing() {
        let meta = transform_meta();
        let avail = map(&[("amount", i64_ty())]);
        let c = call("score_risk", "score", &[("amount", "amount")]);
        let diags = validate_transform_call("p", &c, &avail, &meta, Some(&f64_ty()));
        assert!(diags.is_empty(), "expected no diagnostics, got {:?}", diags);
    }

    #[test]
    fn transform_unmapped_input_field_errors() {
        let meta = transform_meta();
        let avail = map(&[("other", i64_ty())]);
        let c = call("score_risk", "score", &[]); // no mapping
        let diags = validate_transform_call("p", &c, &avail, &meta, None);
        assert_eq!(codes(&diags), vec!["PLUGIN_INPUT_UNMAPPED"]);
        assert!(matches!(diags[0].level, DiagnosticLevel::Error));
    }

    #[test]
    fn transform_missing_source_column_errors() {
        let meta = transform_meta();
        let avail = map(&[]);
        let c = call("score_risk", "score", &[("amount", "nope")]);
        let diags = validate_transform_call("p", &c, &avail, &meta, None);
        assert_eq!(codes(&diags), vec!["PLUGIN_INPUT_MISSING_COLUMN"]);
    }

    #[test]
    fn transform_type_mismatch_errors() {
        let meta = transform_meta();
        // amount column is Boolean - incompatible with plugin's i64 expectation.
        let avail = map(&[("amount", Type::Boolean)]);
        let c = call("score_risk", "score", &[("amount", "amount")]);
        let diags = validate_transform_call("p", &c, &avail, &meta, None);
        assert_eq!(codes(&diags), vec!["PLUGIN_INPUT_TYPE_MISMATCH"]);
    }

    #[test]
    fn transform_output_type_mismatch_errors() {
        let meta = transform_meta();
        let avail = map(&[("amount", i64_ty())]);
        let c = call("score_risk", "score", &[("amount", "amount")]);
        // Plugin outputs f64; destination is Boolean -> incompatible.
        let diags = validate_transform_call("p", &c, &avail, &meta, Some(&Type::Boolean));
        assert!(codes(&diags).contains(&"PLUGIN_OUTPUT_TYPE_MISMATCH"));
    }

    #[test]
    fn source_endpoint_missing_column_with_hint() {
        let meta = PluginMetadata {
            name: "stripe".into(),
            version: "1.0".into(),
            plugin_type: PluginType::Source,
            exchange_format: ExchangeFormat::JsonV1,
            input_schema: vec![],
            output_schema: vec![
                PluginField {
                    name: "id".into(),
                    field_type: "string".into(),
                    nullable: false,
                },
                PluginField {
                    name: "amount".into(),
                    field_type: "i64".into(),
                    nullable: false,
                },
            ],
            output_type: None,
            runtime: PluginRuntime::Native,
        };
        let diags = validate_source_endpoint(
            "ingest_charges",
            "stripe",
            &["id".into(), "status".into()],
            &meta,
        );
        assert_eq!(codes(&diags), vec!["PLUGIN_SOURCE_COLUMN_MISSING"]);
        let suggestion = diags[0].suggestion.as_ref().unwrap();
        assert!(suggestion.contains("id"));
        assert!(suggestion.contains("amount"));
    }

    #[test]
    fn sink_endpoint_missing_column_errors() {
        let meta = PluginMetadata {
            name: "elastic".into(),
            version: "1.0".into(),
            plugin_type: PluginType::Sink,
            exchange_format: ExchangeFormat::JsonV1,
            input_schema: vec![PluginField {
                name: "doc_id".into(),
                field_type: "string".into(),
                nullable: false,
            }],
            output_schema: vec![],
            output_type: None,
            runtime: PluginRuntime::Native,
        };
        let diags = validate_sink_endpoint("export", "elastic", &map(&[]), &meta);
        assert_eq!(codes(&diags), vec!["PLUGIN_SINK_COLUMN_MISSING"]);
    }

    #[test]
    fn empty_input_schema_emits_info_not_error() {
        let meta = PluginMetadata {
            name: "passthrough".into(),
            version: "1.0".into(),
            plugin_type: PluginType::Transform,
            exchange_format: ExchangeFormat::JsonV1,
            input_schema: vec![],
            output_schema: vec![],
            output_type: Some("string".into()),
            runtime: PluginRuntime::Native,
        };
        let avail = map(&[]);
        let c = call("passthrough", "out", &[]);
        let diags = validate_transform_call("p", &c, &avail, &meta, None);
        assert_eq!(codes(&diags), vec!["PLUGIN_INPUT_SCHEMA_EMPTY"]);
        assert!(matches!(diags[0].level, DiagnosticLevel::Info));
    }
}
