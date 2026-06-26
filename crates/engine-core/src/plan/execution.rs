use crate::{
    context::env::EnvContext,
    plan::{builder::PlanBuilder, env::EnvVarCollector},
};
use model::execution::{
    connection::Connection,
    define::{EnvVar, GlobalDefinitions},
    errors::ConvertError,
    execution_config::ExecutionConfig,
    pipeline::Pipeline,
    plugin::PluginDecl,
};
use serde::{Deserialize, Serialize};
use smql_syntax::ast::doc::SmqlDocument;
use std::{
    collections::HashMap,
    sync::{Arc, OnceLock},
};

/// Top-level execution plan compiled from SMQL AST
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionPlan {
    pub definitions: GlobalDefinitions,
    pub execution_config: ExecutionConfig,
    pub connections: Vec<Connection>,
    pub pipelines: Vec<Pipeline>,
    pub plugins: Vec<PluginDecl>,

    /// Environment variables used throughout the configuration
    #[serde(default)]
    pub env_vars: HashMap<String, EnvVar>,

    /// Source config file path - metadata only, excluded from hash.
    #[serde(skip)]
    pub config_path: String,

    /// Cached plan hash - computed lazily on first access.
    #[serde(skip)]
    hash_cache: OnceLock<String>,
}

impl ExecutionPlan {
    /// Build execution plan from SMQL document
    pub fn build(doc: &SmqlDocument, env: Arc<EnvContext>) -> Result<ExecutionPlan, ConvertError> {
        let mut builder = PlanBuilder::new(env);

        if let Some(def_block) = &doc.define_block {
            builder.global_definitions = builder.extract_definitions(def_block)?;
        }

        let execution_config = if let Some(exec_block) = &doc.execution_block {
            builder.build_execution_config(exec_block)?
        } else {
            ExecutionConfig::default()
        };

        for conn_block in &doc.connections {
            let connection = builder.build_connection(conn_block)?;
            builder
                .connections
                .insert(connection.name.clone(), connection);
        }

        let mut pipelines = Vec::new();
        for pipeline_block in &doc.pipelines {
            let pipeline = builder.build_pipeline(pipeline_block)?;
            pipelines.push(pipeline);
        }

        let mut plugins = Vec::new();
        for plugin_block in &doc.plugins {
            plugins.push(builder.build_plugin(plugin_block)?);
        }

        // Collect all environment variable usage throughout the document
        let mut env_collector = EnvVarCollector::new();
        env_collector.collect_document(doc, |expr| builder.eval_expression(expr).ok());

        Ok(ExecutionPlan {
            definitions: GlobalDefinitions {
                variables: builder.global_definitions,
            },
            execution_config,
            connections: {
                let mut conns: Vec<_> = builder.connections.values().cloned().collect();
                conns.sort_by(|a, b| a.name.cmp(&b.name));
                conns
            },
            pipelines,
            plugins,
            env_vars: env_collector.env_vars,
            config_path: String::new(),
            hash_cache: OnceLock::new(),
        })
    }

    /// Deterministic run ID derived from the plan hash.
    /// Same config always produces the same run_id, enabling pause/resume.
    pub fn run_id(&self) -> String {
        format!("run-{}", &self.hash()[..16])
    }

    pub fn get_connection(&self, name: &str) -> Option<&Connection> {
        self.connections.iter().find(|c| c.name == name)
    }

    /// Generate a deterministic hash for the plan.
    ///
    /// The plan contains several HashMaps whose iteration order is
    /// non-deterministic. We canonicalize to sorted-key JSON so that
    /// the same logical config always produces the same hash - this is
    /// critical for pause/resume which derives run_id from the hash.
    ///
    /// The result is cached after the first call.
    pub fn hash(&self) -> &str {
        self.hash_cache.get_or_init(|| {
            use sha2::{Digest, Sha256};

            let value = serde_json::to_value(self).unwrap();
            let canonical = canonical_json(&value);

            let mut hasher = Sha256::new();
            hasher.update(canonical.as_bytes());
            format!("{:x}", hasher.finalize())
        })
    }
}

/// Produce a JSON string with all object keys sorted recursively.
fn canonical_json(value: &serde_json::Value) -> String {
    let mut buf = String::new();
    write_canonical(value, &mut buf);
    buf
}

fn write_canonical(value: &serde_json::Value, buf: &mut String) {
    use std::fmt::Write;

    match value {
        serde_json::Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();

            buf.push('{');
            for (i, key) in keys.iter().enumerate() {
                if i > 0 {
                    buf.push(',');
                }
                write!(buf, "\"{}\":", key).unwrap();
                write_canonical(&map[*key], buf);
            }
            buf.push('}');
        }
        serde_json::Value::Array(arr) => {
            buf.push('[');
            for (i, item) in arr.iter().enumerate() {
                if i > 0 {
                    buf.push(',');
                }
                write_canonical(item, buf);
            }
            buf.push(']');
        }
        // Scalars: use serde_json's own formatting (already deterministic)
        other => {
            write!(buf, "{}", other).unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{context::env::EnvContext, plan::execution::ExecutionPlan};
    use model::{core::value::Value, execution::pipeline::WriteMode};
    use smql_syntax::builder::parse;
    use std::sync::Arc;

    fn build_plan(smql: &str) -> ExecutionPlan {
        let doc = parse(smql).expect("Failed to parse SMQL");
        ExecutionPlan::build(&doc, Arc::new(EnvContext::empty())).expect("Failed to build plan")
    }

    fn build_plan_with_env(smql: &str, vars: &[(&str, &str)]) -> ExecutionPlan {
        let mut env = EnvContext::empty();
        for (k, v) in vars {
            env.set(k.to_string(), v.to_string());
        }
        let doc = parse(smql).expect("Failed to parse SMQL");
        ExecutionPlan::build(&doc, Arc::new(env)).expect("Failed to build plan")
    }

    #[test]
    fn test_plugin_block_builds_decl_with_config_json() {
        let plan = build_plan(
            r#"
            plugin "stripe_src" {
                path = "./plugins/stripe.wasm"
                allow_http = true
                fuel_limit = 50000000
                config {
                    base_url = "https://api.stripe.com"
                    page_size = 100
                }
            }
        "#,
        );

        assert_eq!(plan.plugins.len(), 1);
        let p = &plan.plugins[0];
        assert_eq!(p.name, "stripe_src");
        assert_eq!(p.path.to_str(), Some("./plugins/stripe.wasm"));
        assert!(p.allow_http);
        assert!(p.allow_log, "allow_log defaults to true");
        assert_eq!(p.fuel_limit, Some(50_000_000));

        let cfg: serde_json::Value =
            serde_json::from_slice(p.config_json.as_ref().expect("config_json populated")).unwrap();
        assert_eq!(cfg["base_url"], "https://api.stripe.com");
        assert_eq!(cfg["page_size"], 100);
    }

    #[test]
    fn test_pipeline_plugin_transform_and_wasm_filter_are_wired() {
        use model::execution::pipeline::{ValidationAction, ValidationKind as RuleKind};

        let plan = build_plan(
            r#"
            connection "src" { driver = "mysql" host = "localhost" }
            connection "dst" { driver = "postgres" host = "localhost" }
            pipeline "p" {
                from { connection = connection.src table = "charges" }
                to   { connection = connection.dst table = "charges" }
                select {
                    score = plugin.score_risk({ amount: charges.amount, country: charges.country })
                    keep  = charges.id
                }
                validate {
                    rule "fraud_screen" {
                        filter  = plugin.check_fraud({ amount: charges.amount })
                        on_fail = skip
                    }
                }
            }
        "#,
        );

        let pipe = &plan.pipelines[0];

        // Plugin transform extracted from the select block.
        assert_eq!(pipe.plugin_transforms.len(), 1);
        let pt = &pipe.plugin_transforms[0];
        assert_eq!(pt.plugin_name, "score_risk");
        assert_eq!(pt.output_column, "score");
        assert_eq!(pt.input_mapping.get("amount"), Some(&"amount".to_string()));
        assert_eq!(
            pt.input_mapping.get("country"),
            Some(&"country".to_string())
        );

        // The plugin field is NOT also a regular expression transformation;
        // the plain `keep` field still is.
        let targets: Vec<&str> = pipe
            .transformations
            .iter()
            .map(|t| t.target_field.as_str())
            .collect();
        assert!(targets.contains(&"keep"));
        assert!(!targets.contains(&"score"));

        // WASM filter rule wired into validations.
        let wasm = pipe
            .validations
            .iter()
            .find(|r| matches!(r.kind, RuleKind::WasmFilter { .. }))
            .expect("wasm filter validation");
        assert!(matches!(wasm.action, ValidationAction::Skip));
        if let RuleKind::WasmFilter {
            plugin_name,
            input_mapping,
        } = &wasm.kind
        {
            assert_eq!(plugin_name, "check_fraud");
            assert_eq!(input_mapping.get("amount"), Some(&"amount".to_string()));
        }
    }

    #[test]
    fn test_plugin_block_requires_path() {
        let doc = parse(r#"plugin "broken" { allow_http = true }"#).unwrap();
        let result = ExecutionPlan::build(&doc, Arc::new(EnvContext::empty()));
        assert!(result.is_err(), "plugin without path should fail to build");
    }

    /// Same config parsed multiple times must always produce the same run_id.
    #[test]
    fn test_run_id_is_stable_across_builds() {
        let smql = r#"
            connection "src" { driver = "mysql" host = "localhost" }
            connection "dst" { driver = "postgres" host = "localhost" }
            pipeline "p1" {
                from { connection = connection.src table = "t1" }
                to   { connection = connection.dst table = "t1" }
            }
        "#;

        let ids: Vec<String> = (0..50).map(|_| build_plan(smql).run_id()).collect();
        assert!(
            ids.windows(2).all(|w| w[0] == w[1]),
            "run_id varied across 50 builds: found {:?} and {:?}",
            ids.first(),
            ids.last()
        );
    }

    /// Connection declaration order in SMQL must not affect run_id.
    /// Connections are stored in a HashMap internally - the sort-by-name
    /// in build() ensures deterministic serialization.
    #[test]
    fn test_run_id_stable_regardless_of_connection_declaration_order() {
        let smql_ab = r#"
            connection "aaa" { driver = "mysql" host = "host1" }
            connection "zzz" { driver = "postgres" host = "host2" }
            pipeline "p1" {
                from { connection = connection.aaa table = "t1" }
                to   { connection = connection.zzz table = "t1" }
            }
        "#;
        let smql_ba = r#"
            connection "zzz" { driver = "postgres" host = "host2" }
            connection "aaa" { driver = "mysql" host = "host1" }
            pipeline "p1" {
                from { connection = connection.aaa table = "t1" }
                to   { connection = connection.zzz table = "t1" }
            }
        "#;

        let id_ab = build_plan(smql_ab).run_id();
        let id_ba = build_plan(smql_ba).run_id();
        assert_eq!(id_ab, id_ba, "connection declaration order affected run_id");
    }

    /// Different connection URLs must produce different run_ids.
    #[test]
    fn test_run_id_differs_for_different_connections() {
        let smql_a = r#"
            connection "db" { driver = "postgres" url = "postgres://localhost/db_a" }
            pipeline "p" {
                from { connection = connection.db table = "t" }
                to   { connection = connection.db table = "t2" }
            }
        "#;
        let smql_b = r#"
            connection "db" { driver = "postgres" url = "postgres://localhost/db_b" }
            pipeline "p" {
                from { connection = connection.db table = "t" }
                to   { connection = connection.db table = "t2" }
            }
        "#;

        assert_ne!(build_plan(smql_a).run_id(), build_plan(smql_b).run_id());
    }

    /// Different env var values must produce different run_ids.
    #[test]
    fn test_run_id_differs_for_different_env_values() {
        let smql = r#"
            connection "db" { driver = "postgres" url = env("DB_URL", "fallback") }
            pipeline "p" {
                from { connection = connection.db table = "t" }
                to   { connection = connection.db table = "t2" }
            }
        "#;

        let id_a = build_plan_with_env(smql, &[("DB_URL", "postgres://host_a/db")]).run_id();
        let id_b = build_plan_with_env(smql, &[("DB_URL", "postgres://host_b/db")]).run_id();
        assert_ne!(
            id_a, id_b,
            "different env values should produce different run_ids"
        );
    }

    /// config_path is metadata - must not affect the hash.
    #[test]
    fn test_run_id_not_affected_by_config_path() {
        let smql = r#"
            connection "db" { driver = "postgres" host = "localhost" }
            pipeline "p" {
                from { connection = connection.db table = "t" }
                to   { connection = connection.db table = "t2" }
            }
        "#;

        let mut plan_a = build_plan(smql);
        plan_a.config_path = "/home/user/project/migration.smql".to_string();

        let mut plan_b = build_plan(smql);
        plan_b.config_path = "/tmp/other.smql".to_string();

        assert_eq!(
            plan_a.run_id(),
            plan_b.run_id(),
            "config_path should not affect run_id"
        );
    }

    /// Different pipeline definitions must produce different run_ids.
    #[test]
    fn test_run_id_differs_for_different_pipelines() {
        let smql_a = r#"
            connection "db" { driver = "postgres" host = "localhost" }
            pipeline "p" {
                from { connection = connection.db table = "users" }
                to   { connection = connection.db table = "users_copy" }
            }
        "#;
        let smql_b = r#"
            connection "db" { driver = "postgres" host = "localhost" }
            pipeline "p" {
                from { connection = connection.db table = "orders" }
                to   { connection = connection.db table = "orders_copy" }
            }
        "#;

        assert_ne!(build_plan(smql_a).run_id(), build_plan(smql_b).run_id());
    }

    /// Plans with definitions using HashMaps must hash deterministically.
    #[test]
    fn test_run_id_stable_with_definitions() {
        let smql = r#"
            define {
                rate = 1.5
                prefix = "prod"
            }
            connection "db" { driver = "postgres" host = "localhost" }
            pipeline "p" {
                from { connection = connection.db table = "t" }
                to   { connection = connection.db table = "t2" }
                select { val = amount * define.rate }
            }
        "#;

        let ids: Vec<String> = (0..20).map(|_| build_plan(smql).run_id()).collect();
        assert!(
            ids.windows(2).all(|w| w[0] == w[1]),
            "run_id with definitions was non-deterministic"
        );
    }

    /// Plans with settings (HashMap) must hash deterministically.
    #[test]
    fn test_run_id_stable_with_settings() {
        let smql = r#"
            connection "db" { driver = "postgres" host = "localhost" }
            pipeline "p" {
                from { connection = connection.db table = "t" }
                to   { connection = connection.db table = "t2" }
                settings {
                    batch_size = 5000
                    create_missing_tables = true
                }
            }
        "#;

        let ids: Vec<String> = (0..20).map(|_| build_plan(smql).run_id()).collect();
        assert!(
            ids.windows(2).all(|w| w[0] == w[1]),
            "run_id with settings was non-deterministic"
        );
    }

    /// Many connections (higher chance of HashMap reordering) must still be stable.
    #[test]
    fn test_run_id_stable_with_many_connections() {
        let smql = r#"
            connection "alpha"   { driver = "mysql"    host = "h1" }
            connection "bravo"   { driver = "postgres" host = "h2" }
            connection "charlie" { driver = "mysql"    host = "h3" }
            connection "delta"   { driver = "postgres" host = "h4" }
            pipeline "p" {
                from { connection = connection.alpha table = "t" }
                to   { connection = connection.delta table = "t" }
            }
        "#;

        let ids: Vec<String> = (0..50).map(|_| build_plan(smql).run_id()).collect();
        assert!(
            ids.windows(2).all(|w| w[0] == w[1]),
            "run_id with many connections was non-deterministic"
        );
    }

    /// Whitespace/formatting changes in SMQL produce the same AST and same run_id.
    #[test]
    fn test_run_id_not_affected_by_whitespace() {
        let compact = r#"connection "db" { driver = "postgres" host = "localhost" }
pipeline "p" { from { connection = connection.db table = "t" } to { connection = connection.db table = "t2" } }"#;

        let spacious = r#"
            connection "db" {
                driver = "postgres"
                host   = "localhost"
            }

            pipeline "p" {
                from {
                    connection = connection.db
                    table      = "t"
                }
                to {
                    connection = connection.db
                    table      = "t2"
                }
            }
        "#;

        assert_eq!(
            build_plan(compact).run_id(),
            build_plan(spacious).run_id(),
            "whitespace differences should not affect run_id"
        );
    }

    #[test]
    fn test_full_document_conversion() {
        let input = r#"
define {
    tax_rate = 1.4
}

connection "postgres_prod" {
    driver = "postgres"
    host = "localhost"
}

pipeline "copy_customers" {
    from {
        connection = connection.postgres_prod
        table = "customers"
    }

    to {
        connection = connection.postgres_prod
        table = "customers_copy"
        mode = "insert"
    }

    select {
        id = id
        total = amount * define.tax_rate
    }
}
        "#;

        let doc = parse(input).expect("Failed to parse SMQL");
        let plan = ExecutionPlan::build(&doc, Arc::new(EnvContext::empty()))
            .expect("Failed to build execution plan");

        // Check definitions
        assert_eq!(plan.definitions.variables.len(), 1);
        assert_eq!(
            plan.definitions
                .variables
                .get("tax_rate")
                .map(|def| &def.value),
            Some(&Value::Float(1.4))
        );

        // Check connections
        assert_eq!(plan.connections.len(), 1);
        assert_eq!(plan.connections[0].name, "postgres_prod");
        assert_eq!(plan.connections[0].driver, "postgres");

        // Check pipelines
        assert_eq!(plan.pipelines.len(), 1);
        assert_eq!(plan.pipelines[0].name, "copy_customers");
        assert_eq!(plan.pipelines[0].source.table, "customers");
        assert_eq!(plan.pipelines[0].destination.table, "customers_copy");
        assert!(matches!(
            plan.pipelines[0].destination.mode,
            WriteMode::Insert
        ));

        // Check transformations
        assert_eq!(plan.pipelines[0].transformations.len(), 2);
        assert_eq!(plan.pipelines[0].transformations[0].target_field, "id");
        assert_eq!(plan.pipelines[0].transformations[1].target_field, "total");
    }
}
