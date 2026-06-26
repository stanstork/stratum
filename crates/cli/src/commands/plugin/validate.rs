use super::shared::{caps_from_decl, limits_for};
use crate::{config, error::CliError};
use engine_core::plan::execution::ExecutionPlan;
use engine_processing::EnvContext;
use engine_wasm::{
    runtime::engine::{WasmEngine, WasmEngineConfig},
    schema::{PluginMetadata, PluginType},
};
use model::execution::{connection::Connection, pipeline::ValidationKind, plugin::PluginDecl};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

/// `stratum plugin validate -c <config.smql>` - confirm every declared plugin
/// loads and initializes, then cross-check each plugin's input schema against
/// how the pipelines actually use it.
pub async fn run(
    config_path: Option<String>,
    as_json: bool,
    env: Arc<EnvContext>,
) -> Result<(), CliError> {
    let path = config::resolve_path(config_path)?;
    let plan = config::load_plan(&path, false, env).await?;

    if plan.plugins.is_empty() {
        return Err(CliError::UserMessage(format!("{path} declares no plugins")));
    }

    // Load + instantiate each declared plugin.
    let mut engine = WasmEngine::new(WasmEngineConfig::default())?;
    let mut reports = Vec::new();
    let mut metas: HashMap<String, PluginMetadata> = HashMap::new();
    let mut all_ok = true;

    for decl in &plan.plugins {
        let (report, meta) = validate_one(&mut engine, decl);
        if report.error.is_some() {
            all_ok = false;
        }
        if let Some(m) = meta {
            metas.insert(decl.name.clone(), m);
        }
        reports.push(report);
    }

    // Cross-check schema/role against pipeline usage.
    let (errors, warnings) = cross_check(&plan, &metas);
    if !errors.is_empty() {
        all_ok = false;
    }

    if as_json {
        let out = serde_json::json!({
            "plugins": reports,
            "errors": errors,
            "warnings": warnings,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        for r in &reports {
            match &r.error {
                None => println!(
                    "✓ {:<16} [{}] {} v{}",
                    r.name,
                    r.role.as_deref().unwrap_or("?"),
                    r.resolved_name.as_deref().unwrap_or("?"),
                    r.version.as_deref().unwrap_or("?"),
                ),
                Some(e) => println!("✗ {:<16} {}", r.name, e),
            }
        }
        for w in &warnings {
            println!("⚠ {w}");
        }
        for e in &errors {
            println!("✗ {e}");
        }
    }

    if all_ok {
        Ok(())
    } else {
        Err(CliError::UserMessage("plugin validation failed".into()))
    }
}

#[derive(serde::Serialize)]
struct PluginReport {
    name: String,
    resolved_name: Option<String>,
    role: Option<String>,
    version: Option<String>,
    runtime: Option<String>,
    error: Option<String>,
}

fn validate_one(
    engine: &mut WasmEngine,
    decl: &PluginDecl,
) -> (PluginReport, Option<PluginMetadata>) {
    let mut report = PluginReport {
        name: decl.name.clone(),
        resolved_name: None,
        role: None,
        version: None,
        runtime: None,
        error: None,
    };

    match validate_inner(engine, decl, &mut report) {
        Ok(meta) => (report, Some(meta)),
        Err(e) => {
            report.error = Some(e);
            (report, None)
        }
    }
}

fn validate_inner(
    engine: &mut WasmEngine,
    decl: &PluginDecl,
    report: &mut PluginReport,
) -> Result<PluginMetadata, String> {
    if !decl.path.exists() {
        return Err(format!("missing file: {}", decl.path.display()));
    }

    // Compile (catches malformed wasm / missing memory export).
    let module = engine
        .load_module(&decl.path)
        .map_err(|e| format!("load failed: {e}"))?;

    // Metadata (cheap, no init) - gives role/version/runtime.
    let meta = engine
        .read_metadata(&module)
        .map_err(|e| format!("metadata failed: {e}"))?;
    report.resolved_name = Some(meta.name.clone());
    report.role = Some(format!("{:?}", meta.plugin_type));
    report.version = Some(meta.version.clone());
    report.runtime = Some(format!("{:?}", meta.runtime));

    // Full instantiate with runtime-appropriate limits - catches init-time
    // failures (e.g. a JS bundle that throws on load).
    let caps = caps_from_decl(decl);
    let limits = limits_for(&meta, Some(decl));
    engine
        .instantiate(
            &module,
            decl.name.clone(),
            caps,
            limits,
            decl.config_json.as_deref(),
        )
        .map_err(|e| format!("init failed: {e}"))?;

    Ok(meta)
}

/// Cross-check each plugin's declared input schema and role against how the
/// pipelines reference it. Returns (errors, warnings).
fn cross_check(
    plan: &ExecutionPlan,
    metas: &HashMap<String, PluginMetadata>,
) -> (Vec<String>, Vec<String>) {
    let mut errors = Vec::new();
    let mut warnings = Vec::new();
    let mut used: HashSet<String> = HashSet::new();

    for pipe in &plan.pipelines {
        // Source / sink plugin endpoints (`connection { driver = "wasm" plugin = "..." }`).
        for (conn, role, expected) in [
            (&pipe.source.connection, "source", PluginType::Source),
            (
                &pipe.destination.connection,
                "destination",
                PluginType::Sink,
            ),
        ] {
            let Some(plugin) = wasm_plugin(conn) else {
                continue;
            };
            used.insert(plugin.clone());
            let Some(meta) = metas.get(&plugin) else {
                errors.push(format!(
                    "pipeline '{}' uses {} plugin '{}' which failed to load or isn't declared",
                    pipe.name, role, plugin
                ));
                continue;
            };
            if meta.plugin_type != expected {
                errors.push(format!(
                    "pipeline '{}': '{}' is a {:?}, but is used as a {} endpoint",
                    pipe.name, plugin, meta.plugin_type, role
                ));
            }
        }

        // Transform plugins.
        for ptc in &pipe.plugin_transforms {
            used.insert(ptc.plugin_name.clone());
            let Some(meta) = metas.get(&ptc.plugin_name) else {
                errors.push(format!(
                    "pipeline '{}' uses plugin '{}' which failed to load or isn't declared",
                    pipe.name, ptc.plugin_name
                ));
                continue;
            };
            if meta.plugin_type != PluginType::Transform {
                errors.push(format!(
                    "pipeline '{}': '{}' is a {:?}, but is used as a transform",
                    pipe.name, ptc.plugin_name, meta.plugin_type
                ));
            }
            check_fields(
                &pipe.name,
                &ptc.plugin_name,
                meta,
                &ptc.input_mapping,
                &mut errors,
                &mut warnings,
            );
        }

        // WASM filter plugins (in validation rules).
        for rule in &pipe.validations {
            if let ValidationKind::WasmFilter {
                plugin_name,
                input_mapping,
            } = &rule.kind
            {
                used.insert(plugin_name.clone());
                let Some(meta) = metas.get(plugin_name) else {
                    errors.push(format!(
                        "pipeline '{}' uses filter plugin '{}' which failed to load or isn't declared",
                        pipe.name, plugin_name
                    ));
                    continue;
                };
                if meta.plugin_type != PluginType::Filter {
                    errors.push(format!(
                        "pipeline '{}': '{}' is a {:?}, but is used as a filter",
                        pipe.name, plugin_name, meta.plugin_type
                    ));
                }
                check_fields(
                    &pipe.name,
                    plugin_name,
                    meta,
                    input_mapping,
                    &mut errors,
                    &mut warnings,
                );
            }
        }
    }

    // Declared but never referenced.
    for decl in &plan.plugins {
        if !used.contains(decl.name.as_str()) {
            warnings.push(format!(
                "plugin '{}' is declared but not used by any pipeline",
                decl.name
            ));
        }
    }

    (errors, warnings)
}

/// The plugin name carried by a wasm connection's `plugin` property.
fn wasm_plugin(conn: &Connection) -> Option<String> {
    conn.driver
        .eq_ignore_ascii_case("wasm")
        .then(|| conn.properties.get_string("plugin"))
        .flatten()
}

/// Every declared input field must be supplied by the mapping; mapping keys that
/// aren't declared fields are a warning (typo / stale config).
fn check_fields(
    pipe: &str,
    plugin: &str,
    meta: &PluginMetadata,
    mapping: &HashMap<String, String>,
    errors: &mut Vec<String>,
    warnings: &mut Vec<String>,
) {
    let mapped: HashSet<&str> = mapping.keys().map(String::as_str).collect();
    for f in &meta.input_schema {
        if !mapped.contains(f.name.as_str()) {
            errors.push(format!(
                "pipeline '{pipe}': plugin '{plugin}' input field '{}' is not provided by its mapping",
                f.name
            ));
        }
    }
    let declared: HashSet<&str> = meta.input_schema.iter().map(|f| f.name.as_str()).collect();
    for k in mapping.keys() {
        if !declared.contains(k.as_str()) {
            warnings.push(format!(
                "pipeline '{pipe}': plugin '{plugin}' mapping references unknown input field '{k}'"
            ));
        }
    }
}
