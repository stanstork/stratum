use model::{
    core::value::Value,
    execution::define::{DefinitionSource, EnvVar},
};
use smql_syntax::ast::{
    block::{ConnectionBlock, DefineBlock, ExecutionBlock},
    doc::SmqlDocument,
    expr::{Expression, ExpressionKind},
    literal::Literal,
    pipeline::PipelineBlock,
};
use std::collections::HashMap;

/// Collects environment variable usage across an SMQL configuration
pub struct EnvVarCollector {
    pub env_vars: HashMap<String, EnvVar>,
}

impl EnvVarCollector {
    pub fn new() -> Self {
        Self {
            env_vars: HashMap::new(),
        }
    }

    /// Collect all environment variable usage in an entire SMQL document
    pub fn collect_document<F>(&mut self, document: &SmqlDocument, eval_fn: F)
    where
        F: Fn(&Expression) -> Option<Value>,
    {
        // Collect from define block
        if let Some(define_block) = &document.define_block {
            self.collect_from_define_block(define_block, &eval_fn);
        }

        // Collect from execution block
        if let Some(execution_block) = &document.execution_block {
            self.collect_from_execution_block(execution_block, &eval_fn);
        }

        // Collect from all connection blocks
        for connection_block in &document.connections {
            self.collect_from_connection_block(connection_block, &eval_fn);
        }

        // Collect from all pipeline blocks
        for pipeline_block in &document.pipelines {
            self.collect_from_pipeline_block(pipeline_block, &eval_fn);
        }
    }

    /// Collect env vars from a define block
    pub fn collect_from_define_block(
        &mut self,
        def_block: &DefineBlock,
        eval_fn: impl Fn(&Expression) -> Option<Value>,
    ) {
        for attr in &def_block.attributes {
            let source = Self::analyze_value_source(&attr.value);

            if let Some(value) = eval_fn(&attr.value) {
                self.record_env_var(&attr.key.name, &value, &source);
            }
        }
    }

    /// Collect all env() function calls from a connection block
    pub fn collect_from_connection_block(
        &mut self,
        conn_block: &ConnectionBlock,
        eval_fn: impl Fn(&Expression) -> Option<Value>,
    ) {
        let context = format!("connection.{}", conn_block.name);

        for attr in &conn_block.attributes {
            self.collect_from_expr(&attr.value, Some(&context), &eval_fn);
        }

        for nested in &conn_block.nested_blocks {
            for attr in &nested.attributes {
                self.collect_from_expr(&attr.value, Some(&context), &eval_fn);
            }
        }
    }

    /// Collect all env() function calls from an execution block
    pub fn collect_from_execution_block(
        &mut self,
        exec_block: &ExecutionBlock,
        eval_fn: impl Fn(&Expression) -> Option<Value>,
    ) {
        for attr in &exec_block.attributes {
            self.collect_from_expr(&attr.value, Some("execution"), &eval_fn);
        }
    }

    /// Collect all env() function calls from a pipeline block
    pub fn collect_from_pipeline_block(
        &mut self,
        pipeline_block: &PipelineBlock,
        eval_fn: impl Fn(&Expression) -> Option<Value>,
    ) {
        let context = format!("pipeline.{}", pipeline_block.name);

        // Collect from 'from' block
        if let Some(from) = &pipeline_block.from {
            for attr in &from.attributes {
                self.collect_from_expr(&attr.value, Some(&context), &eval_fn);
            }
        }

        // Collect from 'to' block
        if let Some(to) = &pipeline_block.to {
            for attr in &to.attributes {
                self.collect_from_expr(&attr.value, Some(&context), &eval_fn);
            }
        }

        // Collect from where clauses
        for wc in &pipeline_block.where_clauses {
            for condition in &wc.conditions {
                self.collect_from_expr(condition, Some(&context), &eval_fn);
            }
        }

        // Collect from select block transformations
        if let Some(select) = &pipeline_block.select_block {
            for field in &select.fields {
                self.collect_from_expr(&field.value, Some(&context), &eval_fn);
            }
        }

        // Collect from validation rules
        if let Some(validate) = &pipeline_block.validate_block {
            for check in &validate.checks {
                self.collect_from_expr(&check.body.check, Some(&context), &eval_fn);
            }
        }

        // Collect from on_error block
        if let Some(on_error) = &pipeline_block.on_error_block {
            if let Some(retry) = &on_error.retry {
                for attr in &retry.attributes {
                    self.collect_from_expr(&attr.value, Some(&context), &eval_fn);
                }
            }

            if let Some(failed_rows) = &on_error.failed_rows {
                for attr in &failed_rows.attributes {
                    self.collect_from_expr(&attr.value, Some(&context), &eval_fn);
                }
                for nested in &failed_rows.nested_blocks {
                    for attr in &nested.attributes {
                        self.collect_from_expr(&attr.value, Some(&context), &eval_fn);
                    }
                }
            }
        }

        // Collect from paginate block
        if let Some(paginate) = &pipeline_block.paginate_block {
            for attr in &paginate.attributes {
                self.collect_from_expr(&attr.value, Some(&context), &eval_fn);
            }
        }

        // Collect from settings block
        if let Some(settings) = &pipeline_block.settings_block {
            for attr in &settings.attributes {
                self.collect_from_expr(&attr.value, Some(&context), &eval_fn);
            }
        }
    }

    /// Collect all env() function calls from an expression tree recursively
    fn collect_from_expr(
        &mut self,
        expr: &Expression,
        context_name: Option<&str>,
        eval_fn: &impl Fn(&Expression) -> Option<Value>,
    ) {
        match &expr.kind {
            ExpressionKind::FunctionCall { name, arguments: _ }
                if name.eq_ignore_ascii_case("env") =>
            {
                // Collect this env var usage
                let source = Self::analyze_value_source(expr);

                // Try to evaluate to get the actual value
                if let Some(value) = eval_fn(expr) {
                    let key = context_name
                        .map(|c| format!("{}:env_call", c))
                        .unwrap_or_else(|| format!("env_call_{}", self.env_vars.len()));

                    self.record_env_var(&key, &value, &source);
                }
            }
            ExpressionKind::Binary { left, right, .. } => {
                self.collect_from_expr(left, context_name, eval_fn);
                self.collect_from_expr(right, context_name, eval_fn);
            }
            ExpressionKind::Unary { operand, .. } => {
                self.collect_from_expr(operand, context_name, eval_fn);
            }
            ExpressionKind::FunctionCall { arguments, .. } => {
                // For non-env function calls, check their arguments
                for arg in arguments {
                    self.collect_from_expr(arg, context_name, eval_fn);
                }
            }
            ExpressionKind::Grouped(inner) => {
                self.collect_from_expr(inner, context_name, eval_fn);
            }
            ExpressionKind::WhenExpression {
                branches,
                else_value,
            } => {
                for branch in branches {
                    self.collect_from_expr(&branch.condition, context_name, eval_fn);
                    self.collect_from_expr(&branch.value, context_name, eval_fn);
                }
                if let Some(else_e) = else_value {
                    self.collect_from_expr(else_e, context_name, eval_fn);
                }
            }
            ExpressionKind::IsNull(inner) | ExpressionKind::IsNotNull(inner) => {
                self.collect_from_expr(inner, context_name, eval_fn);
            }
            ExpressionKind::Array(items) => {
                for item in items {
                    self.collect_from_expr(item, context_name, eval_fn);
                }
            }
            // Literals, identifiers, and dot notation don't need recursive collection
            _ => {}
        }
    }

    /// Record environment variable usage
    fn record_env_var(&mut self, def_name: &str, value: &Value, source: &DefinitionSource) {
        match source {
            DefinitionSource::Environment { var_name } => {
                // Check if the env var was actually set
                let was_set = std::env::var(var_name).is_ok();

                self.env_vars.insert(
                    def_name.to_string(),
                    EnvVar {
                        var_name: var_name.clone(),
                        was_set,
                        used_default: false,
                        value: value.clone(),
                    },
                );
            }
            DefinitionSource::EnvironmentWithDefault {
                var_name,
                default_value: _,
            } => {
                // Check if the env var was set or if default was used
                let was_set = std::env::var(var_name).is_ok();

                self.env_vars.insert(
                    def_name.to_string(),
                    EnvVar {
                        var_name: var_name.clone(),
                        was_set,
                        used_default: !was_set,
                        value: value.clone(),
                    },
                );
            }
            DefinitionSource::Literal => {
                // No env var tracking needed for literals
            }
        }
    }

    /// Analyze an expression to determine its value source
    pub fn analyze_value_source(expr: &Expression) -> DefinitionSource {
        match &expr.kind {
            // Function call - check if it's env()
            ExpressionKind::FunctionCall { name, arguments }
                if name.eq_ignore_ascii_case("env") =>
            {
                match arguments.len() {
                    // env("VAR") - required
                    1 => {
                        if let Some(var_name) = Self::extract_string_literal(&arguments[0]) {
                            DefinitionSource::Environment { var_name }
                        } else {
                            DefinitionSource::Literal
                        }
                    }
                    // env("VAR", default) - with fallback
                    2 => {
                        let var_name = Self::extract_string_literal(&arguments[0])
                            .unwrap_or_else(|| "unknown".to_string());
                        let default_value = Self::extract_literal_as_string(&arguments[1])
                            .unwrap_or_else(|| "unknown".to_string());

                        DefinitionSource::EnvironmentWithDefault {
                            var_name,
                            default_value,
                        }
                    }
                    _ => DefinitionSource::Literal,
                }
            }
            // Everything else is a literal
            _ => DefinitionSource::Literal,
        }
    }

    /// Extract string value from a literal expression
    fn extract_string_literal(expr: &Expression) -> Option<String> {
        match &expr.kind {
            ExpressionKind::Literal(Literal::String(s)) => Some(s.clone()),
            _ => None,
        }
    }

    /// Extract any literal value as a string representation
    fn extract_literal_as_string(expr: &Expression) -> Option<String> {
        match &expr.kind {
            ExpressionKind::Literal(Literal::String(s)) => Some(s.clone()),
            ExpressionKind::Literal(Literal::Int(i)) => Some(i.to_string()),
            ExpressionKind::Literal(Literal::Number(n)) => Some(n.to_string()),
            ExpressionKind::Literal(Literal::Boolean(b)) => Some(b.to_string()),
            ExpressionKind::Literal(Literal::Null) => Some("null".to_string()),
            _ => None,
        }
    }
}

impl Default for EnvVarCollector {
    fn default() -> Self {
        Self::new()
    }
}
