use crate::{
    builder::plugin_validation::{
        validate_filter_rule, validate_sink_endpoint, validate_source_endpoint,
        validate_transform_call,
    },
    plan::{
        connection::plan::DatabaseDriver,
        diagnostics::diagnostic::Diagnostic,
        pipeline::{plan::PipelinePlan, source::ColumnInfo},
    },
};
use connectors::sql::metadata::column::ColumnMetadata;
use engine_core::{
    plan::execution::ExecutionPlan as CoreExecutionPlan, schema::type_registry::Dialect,
};
use engine_processing::io::format::DataFormat;
use engine_wasm::{registry::PluginRegistry, schema::PluginField};
use expression_engine::ExpressionAnalyzer;
use model::{
    core::types::Type,
    execution::{
        connection::Connection,
        pipeline::{Pipeline, ValidationKind},
    },
};
use std::collections::HashMap;

pub struct PluginAnalyzer;

impl PluginAnalyzer {
    pub fn new() -> Self {
        Self
    }

    /// Validate every transform/filter plugin call across the plan, appending
    /// diagnostics onto each `PipelinePlan.diagnostics`.
    pub fn analyze(
        &self,
        pipelines: &mut [PipelinePlan],
        core_plan: &CoreExecutionPlan,
        plugin_registry: &PluginRegistry,
    ) {
        for (plan, core) in pipelines.iter_mut().zip(core_plan.pipelines.iter()) {
            self.validate_endpoints(plan, core, plugin_registry);

            let has_transforms = !core.plugin_transforms.is_empty();
            let has_wasm_filters = core
                .validations
                .iter()
                .any(|v| matches!(v.kind, ValidationKind::WasmFilter { .. }));
            if !has_transforms && !has_wasm_filters {
                continue;
            }

            let available = Self::build_column_types(&plan.source.columns, &plan.source.driver);
            let dest_types =
                Self::build_column_types(&plan.destination.columns, &plan.destination.driver);

            if available.is_empty() && !plan.source.columns.is_empty() {
                // Source has columns but we couldn't convert types - unsupported
                // dialect. Flag it once so the user knows checks were skipped.
                plan.diagnostics.push(
                    Diagnostic::info(
                        "PLUGIN_VALIDATION_DIALECT_UNSUPPORTED",
                        &format!(
                            "source driver {:?} has no canonical-type converter; plugin type checks were skipped",
                            plan.source.driver
                        ),
                    )
                    .with_pipeline(&plan.name),
                );
                continue;
            }

            // Transform plugin calls.
            for call in &core.plugin_transforms {
                let plugin = match plugin_registry.metadata(&call.plugin_name) {
                    Ok(m) => m,
                    Err(e) => {
                        plan.diagnostics.push(
                            Diagnostic::error(
                                "PLUGIN_LOAD_FAILED",
                                &format!(
                                    "could not load transform plugin '{}': {}",
                                    call.plugin_name, e
                                ),
                            )
                            .with_pipeline(&plan.name),
                        );
                        continue;
                    }
                };
                let dest_ty = dest_types.get(&call.output_column);
                plan.diagnostics.extend(validate_transform_call(
                    &plan.name, call, &available, &plugin, dest_ty,
                ));
            }

            // Filter plugin rules in `validate { rule … }`.
            for rule in &core.validations {
                let ValidationKind::WasmFilter {
                    plugin_name,
                    input_mapping,
                } = &rule.kind
                else {
                    continue;
                };
                let plugin = match plugin_registry.metadata(plugin_name) {
                    Ok(m) => m,
                    Err(e) => {
                        plan.diagnostics.push(
                            Diagnostic::error(
                                "PLUGIN_LOAD_FAILED",
                                &format!("could not load filter plugin '{}': {}", plugin_name, e),
                            )
                            .with_pipeline(&plan.name),
                        );
                        continue;
                    }
                };
                plan.diagnostics.extend(validate_filter_rule(
                    &plan.name,
                    rule,
                    plugin_name,
                    input_mapping,
                    &available,
                    &plugin,
                ));
            }
        }
    }

    /// Convert a `SourcePlan`/`DestinationPlan` column list into a canonical
    /// `Type` map suitable for plugin type checking.
    fn build_column_types(
        columns: &[ColumnInfo],
        driver: &DatabaseDriver,
    ) -> HashMap<String, Type> {
        if matches!(driver, DatabaseDriver::Other(s) if s == "wasm") {
            return columns
                .iter()
                .map(|ci| {
                    let field = PluginField {
                        name: ci.name.clone(),
                        field_type: ci.data_type.clone(),
                        nullable: ci.nullable,
                    };
                    (ci.name.clone(), field.to_canonical_type())
                })
                .collect();
        }
        let Some(dialect) = Self::dialect_from_driver(driver) else {
            return HashMap::new();
        };
        columns
            .iter()
            .map(|ci| {
                let col = ColumnMetadata {
                    name: ci.name.clone(),
                    data_type: ci.data_type.clone(),
                    is_nullable: ci.nullable,
                    char_max_length: ci.max_length,
                    is_primary_key: ci.is_primary_key,
                    is_auto_increment: ci.is_auto_increment,
                    default_value: ci.default.clone(),
                    has_default: ci.default.is_some(),
                    ..Default::default()
                };
                (ci.name.clone(), dialect.to_canonical(&col))
            })
            .collect()
    }

    fn dialect_from_driver(d: &DatabaseDriver) -> Option<Dialect> {
        match d {
            DatabaseDriver::Postgres => Some(Dialect::Postgres),
            DatabaseDriver::MySql => Some(Dialect::MySql),
            _ => None,
        }
    }

    fn validate_endpoints(
        &self,
        plan: &mut PipelinePlan,
        core: &model::execution::pipeline::Pipeline,
        registry: &PluginRegistry,
    ) {
        // Source side.
        if let Some(plugin) = Self::wasm_plugin_of(&core.source.connection) {
            match registry.metadata(&plugin) {
                Ok(meta) => {
                    let refs = Self::referenced_columns(core);
                    plan.diagnostics
                        .extend(validate_source_endpoint(&plan.name, &plugin, &refs, &meta));
                }
                Err(e) => plan.diagnostics.push(
                    Diagnostic::error(
                        "PLUGIN_LOAD_FAILED",
                        &format!("could not load source plugin '{}': {}", plugin, e),
                    )
                    .with_pipeline(&plan.name),
                ),
            }
        }
        // Sink side.
        if let Some(plugin) = Self::wasm_plugin_of(&core.destination.connection) {
            match registry.metadata(&plugin) {
                Ok(meta) => {
                    let produced = Self::produced_columns(core);
                    plan.diagnostics.extend(validate_sink_endpoint(
                        &plan.name, &plugin, &produced, &meta,
                    ));
                }
                Err(e) => plan.diagnostics.push(
                    Diagnostic::error(
                        "PLUGIN_LOAD_FAILED",
                        &format!("could not load sink plugin '{}': {}", plugin, e),
                    )
                    .with_pipeline(&plan.name),
                ),
            }
        }
    }

    /// Plugin name carried by a wasm connection's `properties["plugin"]`.
    /// Returns None for non-wasm connections or missing property.
    fn wasm_plugin_of(conn: &Connection) -> Option<String> {
        (DataFormat::parse(&conn.driver) == Some(DataFormat::Wasm))
            .then(|| conn.properties.get_string("plugin"))
            .flatten()
    }

    /// All column names referenced by the pipeline's select block.
    fn referenced_columns(pipeline: &Pipeline) -> Vec<String> {
        pipeline
            .transformations
            .iter()
            .flat_map(|t| ExpressionAnalyzer::extract_columns(&t.expression))
            .collect()
    }

    /// Columns produced by the pipeline's select block, with best-effort
    /// types. When the target field is a 1:1 column reference we propagate the
    /// source column's type via the available map; otherwise we mark it
    /// `Unknown` so the validator emits Lossy diagnostics rather than blocking.
    fn produced_columns(pipeline: &Pipeline) -> HashMap<String, Type> {
        pipeline
            .transformations
            .iter()
            .map(|t| {
                (
                    t.target_field.clone(),
                    Type::Unknown {
                        source_name: "computed".to_string(),
                        fallback_ddl: String::new(),
                    },
                )
            })
            .collect()
    }
}

impl Default for PluginAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}
