use crate::builder::analysis::AnalyzerError;
use crate::builder::diagnostics::message_catalog::MessageCatalog;
use crate::{
    builder::{
        analysis::{AnalysisContext, AnalyzerResult, PlanAnalyzer},
        errors::HooksAnalyzerError,
    },
    plan::hooks::{impact::HookImpact, plan::HookStatement, plan::HooksPlan},
};
use async_trait::async_trait;
use connectors::adapter::Adapter;
use model::execution::pipeline::Pipeline;
use once_cell::sync::Lazy;
use std::sync::Arc;
use tracing::{info, warn};

static MESSAGES: Lazy<MessageCatalog> = Lazy::new(|| {
    MessageCatalog::from_toml(
        include_str!("../../../resources/hooks.toml"),
        "Message not found",
    )
});

mod msg {
    pub const WARNINGS: &str = "warnings";
    pub const HINTS: &str = "hints";
    pub const OPERATIONS: &str = "operations";
}

fn get_msg(cat: &str, key: &str) -> String {
    MESSAGES.get_message(cat, key)
}

/// Analyzes lifecycle hooks (pre/post SQL statements) to determine performance and safety impact.
pub struct HooksAnalyzer {
    adapter: Arc<Adapter>,
}

impl HooksAnalyzer {
    pub fn new(adapter: &Adapter) -> Self {
        Self {
            adapter: Arc::new(adapter.clone()),
        }
    }

    /// Primary entry point for analyzing all hooks in a pipeline.
    pub async fn analyze_pipeline_hooks(
        &self,
        pipeline: &Pipeline,
    ) -> Result<HooksPlan, HooksAnalyzerError> {
        let lifecycle = match &pipeline.lifecycle {
            Some(l) => l,
            None => return Ok(HooksPlan::default()),
        };

        let mut before = Vec::new();
        for sql in &lifecycle.before {
            before.push(self.process_statement(sql, pipeline).await?);
        }

        let mut after = Vec::new();
        for sql in &lifecycle.after {
            after.push(self.process_statement(sql, pipeline).await?);
        }

        info!(
            target: "analyzer",
            pipeline = %pipeline.name,
            before = before.len(),
            after = after.len(),
            "Lifecycle hooks analysis complete"
        );

        Ok(HooksPlan {
            before_count: before.len(),
            after_count: after.len(),
            before,
            after,
        })
    }

    /// Processes a single SQL statement, determining its impact and generating safety warnings.
    async fn process_statement(
        &self,
        sql: &str,
        pipeline: &Pipeline,
    ) -> Result<HookStatement, HooksAnalyzerError> {
        let sql_trimmed = sql.trim();
        let sql_upper = sql_trimmed.to_uppercase();

        // Classify the operation and determine technical impact
        let impact = self.determine_impact(sql_trimmed, &sql_upper).await?;

        // Generate human-readable warnings based on the impact
        let warnings = self.generate_warnings(&sql_upper, &impact);

        // Log any significant safety risks
        for warning in &warnings {
            warn!(pipeline = %pipeline.name, sql = %sql_trimmed, warning = %warning, "Hook safety risk detected");
        }

        Ok(HookStatement {
            sql: sql_trimmed.to_string(),
            connection: pipeline.destination.connection.name.clone(),
            impact: Some(impact),
            warnings,
        })
    }

    async fn determine_impact(
        &self,
        raw_sql: &str,
        sql_upper: &str,
    ) -> Result<HookImpact, HooksAnalyzerError> {
        // DDL / Schema Operations
        if sql_upper.starts_with("CREATE")
            || sql_upper.starts_with("ALTER")
            || sql_upper.starts_with("DROP")
        {
            if sql_upper.contains("INDEX") {
                return Ok(self.classify_index_op(sql_upper));
            }
            if sql_upper.contains("TABLE") {
                return Ok(self.classify_table_op(sql_upper));
            }
            if sql_upper.contains("TRIGGER") {
                return Ok(HookImpact::TriggerOperation {
                    action: if sql_upper.starts_with("CREATE") {
                        get_msg(msg::OPERATIONS, "create_trigger")
                    } else {
                        get_msg(msg::OPERATIONS, "modify_trigger")
                    },
                });
            }
        }

        // DML / Data Operations
        if ["INSERT", "UPDATE", "DELETE"]
            .iter()
            .any(|k| sql_upper.starts_with(k))
        {
            let operation = sql_upper
                .split_whitespace()
                .next()
                .unwrap_or("DML")
                .to_string();
            let rows = self.estimate_dml_rows(raw_sql).await;
            return Ok(HookImpact::DataOperation {
                operation,
                is_bulk: rows.map(|r| r > 1000).unwrap_or(false),
                estimated_rows: rows,
            });
        }

        // Maintenance / Utilities
        if let Some(op) = ["VACUUM", "ANALYZE", "REINDEX", "OPTIMIZE", "TRUNCATE"]
            .iter()
            .find(|&&k| sql_upper.starts_with(k))
        {
            return Ok(HookImpact::Maintenance {
                operation: op.to_string(),
            });
        }

        Ok(HookImpact::Other {
            description: sql_upper
                .split_whitespace()
                .next()
                .unwrap_or("SQL")
                .to_string(),
        })
    }

    fn classify_table_op(&self, sql_upper: &str) -> HookImpact {
        let parts: Vec<&str> = sql_upper.split_whitespace().collect();
        let operation = parts
            .get(0..2)
            .map(|s| s.join(" "))
            .unwrap_or_else(|| "TABLE OP".into());

        // Find table name while skipping "IF [NOT] EXISTS"
        let mut idx = 2;
        if parts.get(idx) == Some(&"IF") {
            idx += if parts.get(idx + 1) == Some(&"NOT") {
                3
            } else {
                2
            };
        }

        let target = parts.get(idx).cloned().unwrap_or("unknown").to_lowercase();
        HookImpact::SchemaChange { operation, target }
    }

    fn classify_index_op(&self, sql_upper: &str) -> HookImpact {
        HookImpact::IndexOperation {
            is_concurrent: sql_upper.contains("CONCURRENTLY"),
            is_destructive: sql_upper.contains("DROP"),
            hint: if sql_upper.starts_with("CREATE") {
                Some(get_msg(msg::HINTS, "index_create_slow"))
            } else {
                None
            },
        }
    }

    /// Attempts to fetch row estimates using EXPLAIN.
    async fn estimate_dml_rows(&self, sql: &str) -> Option<u64> {
        let explain_sql = format!("EXPLAIN {}", sql);
        let results = self.adapter.get_sql().query_rows(&explain_sql).await.ok()?;

        // Look for common row count columns in EXPLAIN output
        results.first().and_then(|row| {
            row.get_value("rows")
                .as_i64()
                .or_else(|| row.get_value("row_count").as_i64())
                .map(|r| r as u64)
        })
    }

    fn generate_warnings(&self, sql_upper: &str, impact: &HookImpact) -> Vec<String> {
        let mut warnings = Vec::new();

        match impact {
            HookImpact::SchemaChange { .. } => {
                warnings.push(get_msg(msg::WARNINGS, "schema_change"));
            }
            HookImpact::IndexOperation { is_concurrent, .. } if !is_concurrent => {
                warnings.push(get_msg(msg::WARNINGS, "non_concurrent_index"));
            }
            HookImpact::DataOperation {
                estimated_rows,
                is_bulk,
                ..
            } => {
                if *is_bulk {
                    let template = get_msg(msg::WARNINGS, "bulk_operation");
                    warnings
                        .push(template.replace("{rows}", &estimated_rows.unwrap_or(0).to_string()));
                }
                if (sql_upper.contains("DELETE") || sql_upper.contains("UPDATE"))
                    && !sql_upper.contains("WHERE")
                {
                    warnings.push(get_msg(msg::WARNINGS, "dml_no_where"));
                }
            }
            HookImpact::Maintenance { operation } => {
                let key = if operation == "TRUNCATE" {
                    "truncate_destructive"
                } else {
                    "maintenance_slow"
                };
                warnings.push(get_msg(msg::WARNINGS, key));
            }
            _ => {}
        }

        // Final generic safety check
        if sql_upper.contains("DROP")
            && !matches!(
                impact,
                HookImpact::IndexOperation { .. } | HookImpact::TriggerOperation { .. }
            )
        {
            warnings.push(get_msg(msg::WARNINGS, "destructive_drop"));
        }

        warnings
    }
}

#[async_trait]
impl PlanAnalyzer for HooksAnalyzer {
    type Input = Pipeline;
    type Output = HooksPlan;

    fn name(&self) -> &'static str {
        "hooks"
    }

    async fn analyze(
        &self,
        pipeline: &Self::Input,
        _ctx: &AnalysisContext,
    ) -> AnalyzerResult<Self::Output> {
        self.analyze_pipeline_hooks(pipeline)
            .await
            .map_err(|e| AnalyzerError::error("hooks", format!("Analysis error: {}", e)))
    }
}
