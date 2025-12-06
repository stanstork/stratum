use crate::{
    error::ConvertError,
    models::{
        connection::Connection,
        define::GlobalDefinitions,
        expr::{BinaryOp, CompiledExpression},
        pipeline::{
            BackoffStrategy, DataDestination, DataSource, ErrorHandling, FailedRowsAction,
            FailedRowsPolicy, Filter, Join, LifecycleHooks, Pagination, Pipeline, RetryPolicy,
            Transformation, ValidationAction, ValidationRule, ValidationSeverity, WriteMode,
        },
        plan::ExecutionPlan,
    },
};
use model::core::value::Value;
use smql_syntax::ast::{
    block::{ConnectionBlock, DefineBlock},
    doc::SmqlDocument,
    expr::{Expression, ExpressionKind},
    literal::Literal,
    operator::BinaryOperator,
    pipeline::{FromBlock, PipelineBlock, ToBlock},
    validation::ValidationKind,
};
use std::collections::HashMap;

/// Convert validated AST to execution plan
pub struct PlanBuilder {
    // For resolving references
    global_definitions: HashMap<String, Value>,
    connections: HashMap<String, Connection>,
}

impl PlanBuilder {
    pub fn new() -> Self {
        Self {
            global_definitions: HashMap::new(),
            connections: HashMap::new(),
        }
    }

    /// Build execution plan from SMQL document
    pub fn build(mut self, doc: &SmqlDocument) -> Result<ExecutionPlan, ConvertError> {
        if let Some(def_block) = &doc.define_block {
            self.global_definitions = self.extract_definitions(def_block)?;
        }

        for conn_block in &doc.connections {
            let connection = self.build_connection(conn_block)?;
            self.connections.insert(connection.name.clone(), connection);
        }

        let mut pipelines = Vec::new();
        for pipeline_block in &doc.pipelines {
            let pipeline = self.build_pipeline(pipeline_block)?;
            pipelines.push(pipeline);
        }

        Ok(ExecutionPlan {
            definitions: GlobalDefinitions {
                variables: self.global_definitions,
            },
            connections: self.connections.values().cloned().collect(),
            pipelines,
        })
    }

    fn build_connection(&self, conn_block: &ConnectionBlock) -> Result<Connection, ConvertError> {
        let mut properties = HashMap::new();
        let mut nested_configs = HashMap::new();

        for attr in &conn_block.attributes {
            let value = self.eval_expression(&attr.value)?;
            properties.insert(attr.key.name.clone(), value);
        }

        for nested in &conn_block.nested_blocks {
            let mut nested_props = HashMap::new();
            for attr in &nested.attributes {
                let value = self.eval_expression(&attr.value)?;
                nested_props.insert(attr.key.name.clone(), value);
            }
            nested_configs.insert(nested.kind.clone(), nested_props);
        }

        Ok(Connection {
            name: conn_block.name.clone(),
            driver: properties
                .get("driver")
                .and_then(|v| match v {
                    Value::String(s) => Some(s.clone()),
                    _ => None,
                })
                .ok_or_else(|| ConvertError::Connection("Connection missing driver".to_string()))?,
            properties,
            nested_configs,
        })
    }

    fn build_pipeline(&self, pipeline_block: &PipelineBlock) -> Result<Pipeline, ConvertError> {
        let source = self.build_source(&pipeline_block)?;
        let destination = self.build_destination(&pipeline_block)?;
        let dependencies = self.build_dependencies(&pipeline_block)?;
        let transformations = self.build_transformations(&pipeline_block)?;
        let validation_rules = self.build_validation_rules(&pipeline_block)?;
        let error_handling = self.build_error_handling(&pipeline_block)?;
        let lifecycle = self.build_lifecycle(&pipeline_block)?;
        let settings = self.build_settings(&pipeline_block)?;

        Ok(Pipeline {
            name: pipeline_block.name.clone(),
            description: pipeline_block.description.clone(),
            dependencies,
            source,
            destination,
            transformations,
            validations: validation_rules,
            lifecycle: Some(lifecycle),
            error_handling: Some(error_handling),
            settings,
        })
    }

    fn build_source(&self, pipeline_block: &PipelineBlock) -> Result<DataSource, ConvertError> {
        let from = pipeline_block
            .from
            .as_ref()
            .ok_or_else(|| ConvertError::Plan("Pipeline missing from block".to_string()))?;

        let connection = self.resolve_connection_from(from)?;

        let filters = pipeline_block
            .where_clauses
            .iter()
            .flat_map(|wc| {
                wc.conditions.iter().map(|expr| Filter {
                    label: wc.label.clone(),
                    condition: self.compile_expression(expr).unwrap(),
                })
            })
            .collect();

        let joins = if let Some(with_block) = &pipeline_block.with_block {
            with_block
                .joins
                .iter()
                .map(|j| Join {
                    alias: j.alias.name.clone(),
                    table: j.table.name.clone(),
                    condition: j
                        .condition
                        .as_ref()
                        .map(|expr| self.compile_expression(expr).unwrap()),
                })
                .collect()
        } else {
            Vec::new()
        };

        let pagination = pipeline_block.paginate_block.as_ref().map(|p| {
            // Extract pagination config from attributes
            let page_size = p
                .attributes
                .iter()
                .find(|a| a.key.name == "page_size")
                .and_then(|a| self.eval_expression(&a.value).ok())
                .and_then(|v| match v {
                    Value::Usize(n) => Some(n as usize),
                    Value::Int32(n) => Some(n as usize),
                    _ => None,
                })
                .unwrap_or(1000);

            let strategy = p
                .attributes
                .iter()
                .find(|a| a.key.name == "strategy")
                .and_then(|a| self.eval_expression(&a.value).ok())
                .and_then(|v| match v {
                    Value::String(s) => Some(s),
                    _ => None,
                })
                .unwrap_or_else(|| "default".to_string());

            let cursor_field = p
                .attributes
                .iter()
                .find(|a| a.key.name == "cursor_field")
                .and_then(|a| self.eval_expression(&a.value).ok())
                .and_then(|v| match v {
                    Value::String(s) => Some(s),
                    _ => None,
                });

            Pagination {
                strategy,
                page_size,
                cursor_field,
            }
        });

        // Extract table name
        let table = from
            .attributes
            .iter()
            .find(|a| a.key.name == "table")
            .and_then(|a| self.eval_expression(&a.value).ok())
            .and_then(|v| match v {
                Value::String(s) => Some(s),
                _ => None,
            })
            .ok_or_else(|| ConvertError::Plan("From block missing table attribute".to_string()))?;

        Ok(DataSource {
            connection,
            table,
            filters,
            joins,
            pagination,
        })
    }

    fn build_destination(
        &self,
        pipeline_block: &PipelineBlock,
    ) -> Result<DataDestination, ConvertError> {
        let to = pipeline_block
            .to
            .as_ref()
            .ok_or_else(|| ConvertError::Plan("Pipeline missing to block".to_string()))?;

        let connection = self.resolve_connection_to(to)?;

        // Extract table name
        let table = to
            .attributes
            .iter()
            .find(|a| a.key.name == "table")
            .and_then(|a| self.eval_expression(&a.value).ok())
            .and_then(|v| match v {
                Value::String(s) => Some(s),
                _ => None,
            })
            .ok_or_else(|| ConvertError::Plan("To block missing table attribute".to_string()))?;

        let mode = to
            .attributes
            .iter()
            .find(|a| a.key.name == "mode")
            .and_then(|a| self.eval_expression(&a.value).ok())
            .and_then(|v| match v {
                Value::String(s) => match s.as_str() {
                    "insert" => Some(WriteMode::Insert),
                    "update" => Some(WriteMode::Update),
                    "upsert" => Some(WriteMode::Upsert),
                    "replace" => Some(WriteMode::Replace),
                    _ => None,
                },
                _ => None,
            })
            .unwrap_or(WriteMode::Insert);

        Ok(DataDestination {
            connection,
            table,
            mode,
        })
    }

    fn build_dependencies(
        &self,
        pipeline_block: &PipelineBlock,
    ) -> Result<Vec<String>, ConvertError> {
        // Extract dependencies from the 'after' field which contains pipeline names
        if let Some(after) = &pipeline_block.after {
            let mut deps = Vec::new();
            for expr in after {
                if let ExpressionKind::Literal(Literal::String(s)) = &expr.kind {
                    deps.push(s.clone());
                }
            }
            Ok(deps)
        } else {
            Ok(Vec::new())
        }
    }

    fn build_transformations(
        &self,
        pipeline_block: &PipelineBlock,
    ) -> Result<Vec<Transformation>, ConvertError> {
        if let Some(select) = &pipeline_block.select_block {
            Ok(select
                .fields
                .iter()
                .map(|f| Transformation {
                    target_field: f.name.name.clone(),
                    expression: self.compile_expression(&f.value).unwrap(),
                })
                .collect())
        } else {
            Ok(Vec::new())
        }
    }

    fn build_validation_rules(
        &self,
        pipeline_block: &PipelineBlock,
    ) -> Result<Vec<ValidationRule>, ConvertError> {
        if let Some(validate) = &pipeline_block.validate_block {
            Ok(validate
                .checks
                .iter()
                .map(|check| ValidationRule {
                    label: check.label.clone(),
                    severity: match check.kind {
                        ValidationKind::Assert => ValidationSeverity::Assert,
                        ValidationKind::Warn => ValidationSeverity::Warn,
                    },
                    check: self.compile_expression(&check.body.check).unwrap(),
                    message: check.body.message.clone(),
                    action: check
                        .body
                        .action
                        .as_ref()
                        .and_then(|a| match a.as_str() {
                            "skip" => Some(ValidationAction::Skip),
                            "fail" => Some(ValidationAction::Fail),
                            "warn" => Some(ValidationAction::Warn),
                            "continue" => Some(ValidationAction::Continue),
                            _ => None,
                        })
                        .unwrap_or(ValidationAction::Fail),
                })
                .collect())
        } else {
            Ok(Vec::new())
        }
    }

    fn build_error_handling(
        &self,
        pipeline_block: &PipelineBlock,
    ) -> Result<ErrorHandling, ConvertError> {
        if let Some(on_error) = &pipeline_block.on_error_block {
            let retry = on_error.retry.as_ref().map(|r| {
                // Extract retry config from attributes
                let max_attempts = r
                    .attributes
                    .iter()
                    .find(|a| a.key.name == "max_attempts")
                    .and_then(|a| self.eval_expression(&a.value).ok())
                    .and_then(|v| match v {
                        Value::Int32(n) => Some(n as u32),
                        _ => None,
                    })
                    .unwrap_or(3);

                RetryPolicy {
                    max_attempts,
                    delay_ms: 1000,
                    backoff: BackoffStrategy::Exponential,
                }
            });

            let failed_rows = on_error.failed_rows.as_ref().map(|_fr| FailedRowsPolicy {
                action: FailedRowsAction::Log,
                output_table: None,
            });

            Ok(ErrorHandling { retry, failed_rows })
        } else {
            Ok(ErrorHandling {
                retry: None,
                failed_rows: None,
            })
        }
    }

    fn build_lifecycle(
        &self,
        pipeline_block: &PipelineBlock,
    ) -> Result<LifecycleHooks, ConvertError> {
        let before = pipeline_block
            .before_block
            .as_ref()
            .map(|b| b.sql.clone())
            .unwrap_or_default();

        let after = pipeline_block
            .after_block
            .as_ref()
            .map(|b| b.sql.clone())
            .unwrap_or_default();

        Ok(LifecycleHooks { before, after })
    }

    fn build_settings(
        &self,
        pipeline_block: &PipelineBlock,
    ) -> Result<HashMap<String, Value>, ConvertError> {
        if let Some(settings) = &pipeline_block.settings_block {
            let mut settings_map = HashMap::new();
            for attr in &settings.attributes {
                let value = self.eval_expression(&attr.value)?;
                settings_map.insert(attr.key.name.clone(), value);
            }
            Ok(settings_map)
        } else {
            Ok(HashMap::new())
        }
    }

    fn resolve_connection_from(&self, from: &FromBlock) -> Result<String, ConvertError> {
        // Look for connection = connection.name attribute
        for attr in &from.attributes {
            if attr.key.name == "connection" {
                if let ExpressionKind::DotNotation(path) = &attr.value.kind {
                    if path.segments.len() == 2 && path.segments[0] == "connection" {
                        return Ok(path.segments[1].clone());
                    }
                }
            }
        }
        Err(ConvertError::Plan(
            "From block missing connection attribute".to_string(),
        ))
    }

    fn resolve_connection_to(&self, to: &ToBlock) -> Result<String, ConvertError> {
        // Look for connection = connection.name attribute
        for attr in &to.attributes {
            if attr.key.name == "connection" {
                if let ExpressionKind::DotNotation(path) = &attr.value.kind {
                    if path.segments.len() == 2 && path.segments[0] == "connection" {
                        return Ok(path.segments[1].clone());
                    }
                }
            }
        }
        Err(ConvertError::Plan(
            "To block missing connection attribute".to_string(),
        ))
    }

    fn compile_expression(&self, expr: &Expression) -> Result<CompiledExpression, ConvertError> {
        match &expr.kind {
            ExpressionKind::Literal(lit) => match lit {
                Literal::String(s) => Ok(CompiledExpression::Literal(Value::String(s.clone()))),
                Literal::Number(n) => Ok(CompiledExpression::Literal(Value::Float(*n))),
                Literal::Boolean(b) => Ok(CompiledExpression::Literal(Value::Boolean(*b))),
                Literal::Null => Ok(CompiledExpression::Literal(Value::Null)),
            },
            ExpressionKind::Identifier(id) => Ok(CompiledExpression::Identifier(id.clone())),
            ExpressionKind::DotNotation(path) => {
                // Resolve define.X references
                if path.segments[0] == "define" && path.segments.len() == 2 {
                    if let Some(value) = self.global_definitions.get(&path.segments[1]) {
                        return Ok(CompiledExpression::Literal(value.clone()));
                    }
                }
                Ok(CompiledExpression::DotPath(path.segments.clone()))
            }
            ExpressionKind::Binary {
                left,
                operator,
                right,
            } => Ok(CompiledExpression::Binary {
                op: self.convert_binop(*operator),
                left: Box::new(self.compile_expression(left)?),
                right: Box::new(self.compile_expression(right)?),
            }),
            _ => Ok(CompiledExpression::Literal(Value::Null)),
        }
    }

    fn eval_expression(&self, expr: &Expression) -> Result<Value, ConvertError> {
        // Evaluate simple expressions to values
        match &expr.kind {
            ExpressionKind::Literal(lit) => Ok(match lit {
                Literal::String(s) => Value::String(s.clone()),
                Literal::Number(n) => Value::Float(*n),
                Literal::Boolean(b) => Value::Boolean(*b),
                Literal::Null => Value::Null,
            }),
            _ => Err(ConvertError::Expression(format!(
                "cannot evaluate complex expression: {:?}",
                expr
            ))),
        }
    }

    fn extract_definitions(
        &self,
        def_block: &DefineBlock,
    ) -> Result<HashMap<String, Value>, ConvertError> {
        let mut definitions = HashMap::new();
        for attr in &def_block.attributes {
            let value = self.eval_expression(&attr.value)?;
            definitions.insert(attr.key.name.clone(), value);
        }
        Ok(definitions)
    }

    fn convert_binop(&self, op: BinaryOperator) -> BinaryOp {
        match op {
            BinaryOperator::Add => BinaryOp::Add,
            BinaryOperator::Subtract => BinaryOp::Subtract,
            BinaryOperator::Multiply => BinaryOp::Multiply,
            BinaryOperator::Divide => BinaryOp::Divide,
            BinaryOperator::Modulo => BinaryOp::Modulo,
            BinaryOperator::Equal => BinaryOp::Equal,
            BinaryOperator::NotEqual => BinaryOp::NotEqual,
            BinaryOperator::GreaterThan => BinaryOp::GreaterThan,
            BinaryOperator::LessThan => BinaryOp::LessThan,
            BinaryOperator::GreaterOrEqual => BinaryOp::GreaterOrEqual,
            BinaryOperator::LessOrEqual => BinaryOp::LessOrEqual,
            BinaryOperator::And => BinaryOp::And,
            BinaryOperator::Or => BinaryOp::Or,
        }
    }
}
