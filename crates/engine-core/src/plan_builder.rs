use model::{
    core::value::Value,
    execution::{
        connection::Connection,
        errors::ConvertError,
        expr::{BinaryOp, CompiledExpression},
        pipeline::{
            BackoffStrategy, DataDestination, DataSource, ErrorHandling, FailedRowsAction,
            FailedRowsPolicy, Filter, Join, LifecycleHooks, Pagination, Pipeline, RetryPolicy,
            Transformation, ValidationAction, ValidationRule, ValidationSeverity, WriteMode,
        },
        properties::Properties,
    },
};
use smql_syntax::ast::{
    block::{ConnectionBlock, DefineBlock},
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
    pub global_definitions: HashMap<String, Value>,
    pub connections: HashMap<String, Connection>,
}

impl PlanBuilder {
    pub fn new() -> Self {
        Self {
            global_definitions: HashMap::new(),
            connections: HashMap::new(),
        }
    }

    pub fn build_connection(
        &self,
        conn_block: &ConnectionBlock,
    ) -> Result<Connection, ConvertError> {
        let mut properties = Properties::new();
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
                .get_string("driver")
                .ok_or_else(|| ConvertError::Connection("Connection missing driver".to_string()))?,
            properties,
            nested_configs,
        })
    }

    pub fn build_pipeline(&self, pipeline_block: &PipelineBlock) -> Result<Pipeline, ConvertError> {
        let source = self.build_source(pipeline_block)?;
        let destination = self.build_destination(pipeline_block)?;
        let dependencies = self.build_dependencies(pipeline_block)?;
        let transformations = self.build_transformations(pipeline_block)?;
        let validation_rules = self.build_validation_rules(pipeline_block)?;
        let error_handling = self.build_error_handling(pipeline_block)?;
        let lifecycle = self.build_lifecycle(pipeline_block)?;
        let settings = self.build_settings(pipeline_block)?;

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

            let cursor = p
                .attributes
                .iter()
                .find(|a| a.key.name == "cursor")
                .and_then(|a| self.eval_expression(&a.value).ok())
                .and_then(|v| match v {
                    Value::String(s) => Some(s),
                    _ => None,
                })
                .unwrap_or_else(|| "id".to_string());

            let tiebreaker = p
                .attributes
                .iter()
                .find(|a| a.key.name == "tiebreaker")
                .and_then(|a| self.eval_expression(&a.value).ok())
                .and_then(|v| match v {
                    Value::String(s) => Some(s),
                    _ => None,
                });

            let timezone = p
                .attributes
                .iter()
                .find(|a| a.key.name == "timezone")
                .and_then(|a| self.eval_expression(&a.value).ok())
                .and_then(|v| match v {
                    Value::String(s) => Some(s),
                    _ => None,
                });

            Pagination {
                strategy,
                cursor,
                tiebreaker,
                timezone,
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
            connection: self.connections.get(&connection).cloned().ok_or_else(|| {
                ConvertError::Connection(format!("Connection `{}` not found", connection))
            })?,
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
            connection: self.connections.get(&connection).cloned().ok_or_else(|| {
                ConvertError::Connection(format!("Connection `{}` not found", connection))
            })?,
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
                        Value::Float(f) => Some(f as u32),
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
            if attr.key.name == "connection"
                && let ExpressionKind::DotNotation(path) = &attr.value.kind
                && path.segments.len() == 2
                && path.segments[0] == "connection"
            {
                return Ok(path.segments[1].clone());
            }
        }
        Err(ConvertError::Plan(
            "From block missing connection attribute".to_string(),
        ))
    }

    fn resolve_connection_to(&self, to: &ToBlock) -> Result<String, ConvertError> {
        // Look for connection = connection.name attribute
        for attr in &to.attributes {
            if attr.key.name == "connection"
                && let ExpressionKind::DotNotation(path) = &attr.value.kind
                && path.segments.len() == 2
                && path.segments[0] == "connection"
            {
                return Ok(path.segments[1].clone());
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
                if path.segments[0] == "define"
                    && path.segments.len() == 2
                    && let Some(value) = self.global_definitions.get(&path.segments[1])
                {
                    return Ok(CompiledExpression::Literal(value.clone()));
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
            ExpressionKind::FunctionCall { name, arguments } => {
                let compiled_args = arguments
                    .iter()
                    .map(|arg| self.compile_expression(arg))
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(CompiledExpression::FunctionCall {
                    name: name.clone(),
                    args: compiled_args,
                })
            }
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
            ExpressionKind::FunctionCall { name, arguments } if name == "env" => {
                use crate::context::env::{get_env, get_env_or};

                match arguments.len() {
                    1 => {
                        // Required environment variable
                        let var_name = match &arguments[0].kind {
                            ExpressionKind::Literal(Literal::String(s)) => s,
                            _ => {
                                return Err(ConvertError::Expression(
                                    "env() function requires a string literal for variable name"
                                        .to_string(),
                                ));
                            }
                        };

                        get_env(var_name).map(Value::String).ok_or_else(|| {
                            ConvertError::Expression(format!(
                                "Required environment variable '{}' not found",
                                var_name
                            ))
                        })
                    }
                    2 => {
                        // Optional environment variable with default
                        let var_name = match &arguments[0].kind {
                            ExpressionKind::Literal(Literal::String(s)) => s,
                            _ => {
                                return Err(ConvertError::Expression(
                                    "env() function requires a string literal for variable name"
                                        .to_string(),
                                ));
                            }
                        };

                        let default_value = self.eval_expression(&arguments[1])?;
                        let default_str = value_to_string(&default_value);

                        Ok(Value::String(get_env_or(var_name, &default_str)))
                    }
                    _ => Err(ConvertError::Expression(format!(
                        "env() function takes 1 or 2 arguments, got {}",
                        arguments.len()
                    ))),
                }
            }
            _ => Err(ConvertError::Expression(format!(
                "cannot evaluate complex expression: {:?}",
                expr
            ))),
        }
    }

    pub fn extract_definitions(
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

impl Default for PlanBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use smql_syntax::ast::{
        attribute::Attribute,
        dotpath::DotPath,
        ident::Identifier,
        pipeline::{AfterBlock, BeforeBlock, PaginateBlock, SettingsBlock},
        span::Span,
        validation::{OnErrorBlock, RetryBlock, ValidateBlock, ValidationBody, ValidationCheck},
    };

    fn test_span() -> Span {
        Span::new(0, 10, 1, 1)
    }

    fn make_string_expr(s: &str) -> Expression {
        Expression::new(
            ExpressionKind::Literal(Literal::String(s.to_string())),
            test_span(),
        )
    }

    fn make_number_expr(n: f64) -> Expression {
        Expression::new(ExpressionKind::Literal(Literal::Number(n)), test_span())
    }

    fn make_bool_expr(b: bool) -> Expression {
        Expression::new(ExpressionKind::Literal(Literal::Boolean(b)), test_span())
    }

    fn make_ident_expr(s: &str) -> Expression {
        Expression::new(ExpressionKind::Identifier(s.to_string()), test_span())
    }

    fn make_dotpath_expr(segments: Vec<&str>) -> Expression {
        let path = DotPath::new(
            segments.into_iter().map(|s| s.to_string()).collect(),
            test_span(),
        );
        Expression::new(ExpressionKind::DotNotation(path), test_span())
    }

    fn make_attribute(key: &str, value: Expression) -> Attribute {
        Attribute {
            key: Identifier::new(key, test_span()),
            value,
            span: test_span(),
        }
    }

    #[test]
    fn test_extract_definitions() {
        let builder = PlanBuilder::new();
        let def_block = DefineBlock {
            attributes: vec![
                make_attribute("tax_rate", make_number_expr(1.4)),
                make_attribute("country", make_string_expr("US")),
                make_attribute("enabled", make_bool_expr(true)),
            ],
            span: test_span(),
        };

        let result = builder.extract_definitions(&def_block).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result.get("tax_rate"), Some(&Value::Float(1.4)));
        assert_eq!(
            result.get("country"),
            Some(&Value::String("US".to_string()))
        );
        assert_eq!(result.get("enabled"), Some(&Value::Boolean(true)));
    }

    #[test]
    fn test_build_connection_basic() {
        let builder = PlanBuilder::new();
        let conn_block = ConnectionBlock {
            name: "postgres_prod".to_string(),
            attributes: vec![
                make_attribute("driver", make_string_expr("postgres")),
                make_attribute("host", make_string_expr("localhost")),
                make_attribute("port", make_number_expr(5432.0)),
            ],
            nested_blocks: vec![],
            span: test_span(),
        };

        let result = builder.build_connection(&conn_block).unwrap();
        assert_eq!(result.name, "postgres_prod");
        assert_eq!(result.driver, "postgres");
        assert_eq!(result.properties.len(), 3);
        assert_eq!(
            result.properties.get("host"),
            Some(&Value::String("localhost".to_string()))
        );
    }

    #[test]
    fn test_build_dependencies() {
        let builder = PlanBuilder::new();
        let pipeline = PipelineBlock {
            name: "pipeline2".to_string(),
            description: None,
            after: Some(vec![
                make_string_expr("pipeline1"),
                make_string_expr("pipeline0"),
            ]),
            from: None,
            to: None,
            where_clauses: vec![],
            with_block: None,
            select_block: None,
            validate_block: None,
            on_error_block: None,
            paginate_block: None,
            before_block: None,
            after_block: None,
            settings_block: None,
            span: test_span(),
        };

        let deps = builder.build_dependencies(&pipeline).unwrap();
        assert_eq!(deps.len(), 2);
        assert_eq!(deps[0], "pipeline1");
        assert_eq!(deps[1], "pipeline0");
    }

    #[test]
    fn test_build_lifecycle() {
        let builder = PlanBuilder::new();
        let pipeline = PipelineBlock {
            name: "test".to_string(),
            description: None,
            after: None,
            from: None,
            to: None,
            where_clauses: vec![],
            with_block: None,
            select_block: None,
            validate_block: None,
            on_error_block: None,
            paginate_block: None,
            before_block: Some(BeforeBlock {
                sql: vec!["CREATE TABLE IF NOT EXISTS temp".to_string()],
                span: test_span(),
            }),
            after_block: Some(AfterBlock {
                sql: vec!["DROP TABLE temp".to_string()],
                span: test_span(),
            }),
            settings_block: None,
            span: test_span(),
        };

        let lifecycle = builder.build_lifecycle(&pipeline).unwrap();
        assert_eq!(lifecycle.before.len(), 1);
        assert_eq!(lifecycle.before[0], "CREATE TABLE IF NOT EXISTS temp");
        assert_eq!(lifecycle.after.len(), 1);
        assert_eq!(lifecycle.after[0], "DROP TABLE temp");
    }

    #[test]
    fn test_build_settings() {
        let builder = PlanBuilder::new();
        let pipeline = PipelineBlock {
            name: "test".to_string(),
            description: None,
            after: None,
            from: None,
            to: None,
            where_clauses: vec![],
            with_block: None,
            select_block: None,
            validate_block: None,
            on_error_block: None,
            paginate_block: None,
            before_block: None,
            after_block: None,
            settings_block: Some(SettingsBlock {
                attributes: vec![
                    make_attribute("batch_size", make_number_expr(100.0)),
                    make_attribute("parallel", make_bool_expr(true)),
                ],
                span: test_span(),
            }),
            span: test_span(),
        };

        let settings = builder.build_settings(&pipeline).unwrap();
        assert_eq!(settings.len(), 2);
        assert_eq!(settings.get("batch_size"), Some(&Value::Float(100.0)));
        assert_eq!(settings.get("parallel"), Some(&Value::Boolean(true)));
    }

    #[test]
    fn test_compile_expression_literals() {
        let builder = PlanBuilder::new();

        // String literal
        let expr = make_string_expr("hello");
        let compiled = builder.compile_expression(&expr).unwrap();
        match compiled {
            CompiledExpression::Literal(Value::String(s)) => assert_eq!(s, "hello"),
            _ => panic!("Expected string literal"),
        }

        // Number literal
        let expr = make_number_expr(42.5);
        let compiled = builder.compile_expression(&expr).unwrap();
        match compiled {
            CompiledExpression::Literal(Value::Float(n)) => assert_eq!(n, 42.5),
            _ => panic!("Expected number literal"),
        }

        // Boolean literal
        let expr = make_bool_expr(true);
        let compiled = builder.compile_expression(&expr).unwrap();
        match compiled {
            CompiledExpression::Literal(Value::Boolean(b)) => assert!(b),
            _ => panic!("Expected boolean literal"),
        }
    }

    #[test]
    fn test_compile_expression_identifier() {
        let builder = PlanBuilder::new();
        let expr = make_ident_expr("customer_id");
        let compiled = builder.compile_expression(&expr).unwrap();
        match compiled {
            CompiledExpression::Identifier(id) => assert_eq!(id, "customer_id"),
            _ => panic!("Expected identifier"),
        }
    }

    #[test]
    fn test_compile_expression_dotpath() {
        let builder = PlanBuilder::new();
        let expr = make_dotpath_expr(vec!["customers", "email"]);
        let compiled = builder.compile_expression(&expr).unwrap();
        match compiled {
            CompiledExpression::DotPath(segments) => {
                assert_eq!(segments, vec!["customers", "email"]);
            }
            _ => panic!("Expected dotpath"),
        }
    }

    #[test]
    fn test_compile_expression_binary() {
        let builder = PlanBuilder::new();
        let left = make_number_expr(5.0);
        let right = make_number_expr(3.0);
        let expr = Expression::new(
            ExpressionKind::Binary {
                left: Box::new(left),
                operator: BinaryOperator::Add,
                right: Box::new(right),
            },
            test_span(),
        );

        let compiled = builder.compile_expression(&expr).unwrap();
        match compiled {
            CompiledExpression::Binary { op, .. } => {
                assert!(matches!(op, BinaryOp::Add));
            }
            _ => panic!("Expected binary expression"),
        }
    }

    #[test]
    fn test_compile_expression_with_define_reference() {
        let mut builder = PlanBuilder::new();
        builder
            .global_definitions
            .insert("tax_rate".to_string(), Value::Float(1.4));

        let expr = make_dotpath_expr(vec!["define", "tax_rate"]);
        let compiled = builder.compile_expression(&expr).unwrap();
        match compiled {
            CompiledExpression::Literal(Value::Float(n)) => assert_eq!(n, 1.4),
            _ => panic!("Expected resolved define reference"),
        }
    }

    #[test]
    fn test_convert_binop_all_operators() {
        let builder = PlanBuilder::new();

        assert!(matches!(
            builder.convert_binop(BinaryOperator::Add),
            BinaryOp::Add
        ));
        assert!(matches!(
            builder.convert_binop(BinaryOperator::Subtract),
            BinaryOp::Subtract
        ));
        assert!(matches!(
            builder.convert_binop(BinaryOperator::Multiply),
            BinaryOp::Multiply
        ));
        assert!(matches!(
            builder.convert_binop(BinaryOperator::Divide),
            BinaryOp::Divide
        ));
        assert!(matches!(
            builder.convert_binop(BinaryOperator::Equal),
            BinaryOp::Equal
        ));
        assert!(matches!(
            builder.convert_binop(BinaryOperator::NotEqual),
            BinaryOp::NotEqual
        ));
        assert!(matches!(
            builder.convert_binop(BinaryOperator::And),
            BinaryOp::And
        ));
        assert!(matches!(
            builder.convert_binop(BinaryOperator::Or),
            BinaryOp::Or
        ));
    }

    #[test]
    fn test_build_pagination_with_all_fields() {
        let mut builder = PlanBuilder::new();

        // Add a test connection
        let mut properties = Properties::new();
        properties.insert(
            "url".to_string(),
            Value::String("postgres://localhost/test".to_string()),
        );

        builder.connections.insert(
            "postgres".to_string(),
            Connection {
                name: "postgres".to_string(),
                driver: "postgres".to_string(),
                properties,
                nested_configs: HashMap::new(),
            },
        );

        let pipeline = PipelineBlock {
            name: "test".to_string(),
            description: None,
            after: None,
            from: Some(FromBlock {
                attributes: vec![
                    make_attribute(
                        "connection",
                        make_dotpath_expr(vec!["connection", "postgres"]),
                    ),
                    make_attribute("table", make_string_expr("customers")),
                ],
                nested_blocks: vec![],
                span: test_span(),
            }),
            to: None,
            where_clauses: vec![],
            with_block: None,
            select_block: None,
            validate_block: None,
            on_error_block: None,
            paginate_block: Some(PaginateBlock {
                attributes: vec![
                    make_attribute("strategy", make_string_expr("pk")),
                    make_attribute("cursor", make_string_expr("id")),
                ],
                span: test_span(),
            }),
            before_block: None,
            after_block: None,
            settings_block: None,
            span: test_span(),
        };

        let source = builder.build_source(&pipeline).unwrap();
        let pagination = source.pagination.unwrap();
        assert_eq!(pagination.strategy, "pk");
        assert_eq!(pagination.cursor, "id");
    }

    #[test]
    fn test_build_validation_rules() {
        let builder = PlanBuilder::new();
        let pipeline = PipelineBlock {
            name: "test".to_string(),
            description: None,
            after: None,
            from: None,
            to: None,
            where_clauses: vec![],
            with_block: None,
            select_block: None,
            validate_block: Some(ValidateBlock {
                checks: vec![
                    ValidationCheck {
                        kind: ValidationKind::Assert,
                        label: "positive_amount".to_string(),
                        body: ValidationBody {
                            check: Expression::new(
                                ExpressionKind::Binary {
                                    left: Box::new(make_ident_expr("amount")),
                                    operator: BinaryOperator::GreaterThan,
                                    right: Box::new(make_number_expr(0.0)),
                                },
                                test_span(),
                            ),
                            message: "Amount must be positive".to_string(),
                            action: Some("fail".to_string()),
                        },
                        span: test_span(),
                    },
                    ValidationCheck {
                        kind: ValidationKind::Warn,
                        label: "large_amount".to_string(),
                        body: ValidationBody {
                            check: Expression::new(
                                ExpressionKind::Binary {
                                    left: Box::new(make_ident_expr("amount")),
                                    operator: BinaryOperator::LessThan,
                                    right: Box::new(make_number_expr(10000.0)),
                                },
                                test_span(),
                            ),
                            message: "Large amount detected".to_string(),
                            action: Some("continue".to_string()),
                        },
                        span: test_span(),
                    },
                ],
                span: test_span(),
            }),
            on_error_block: None,
            paginate_block: None,
            before_block: None,
            after_block: None,
            settings_block: None,
            span: test_span(),
        };

        let rules = builder.build_validation_rules(&pipeline).unwrap();
        assert_eq!(rules.len(), 2);

        assert_eq!(rules[0].label, "positive_amount");
        assert!(matches!(rules[0].severity, ValidationSeverity::Assert));
        assert!(matches!(rules[0].action, ValidationAction::Fail));

        assert_eq!(rules[1].label, "large_amount");
        assert!(matches!(rules[1].severity, ValidationSeverity::Warn));
        assert!(matches!(rules[1].action, ValidationAction::Continue));
    }

    #[test]
    fn test_build_error_handling() {
        let builder = PlanBuilder::new();
        let pipeline = PipelineBlock {
            name: "test".to_string(),
            description: None,
            after: None,
            from: None,
            to: None,
            where_clauses: vec![],
            with_block: None,
            select_block: None,
            validate_block: None,
            on_error_block: Some(OnErrorBlock {
                retry: Some(RetryBlock {
                    attributes: vec![make_attribute("max_attempts", make_number_expr(5.0))],
                    span: test_span(),
                }),
                failed_rows: None,
                span: test_span(),
            }),
            paginate_block: None,
            before_block: None,
            after_block: None,
            settings_block: None,
            span: test_span(),
        };

        let error_handling = builder.build_error_handling(&pipeline).unwrap();
        let retry = error_handling.retry.unwrap();
        assert_eq!(retry.max_attempts, 5);
        assert_eq!(retry.delay_ms, 1000);
        assert!(matches!(retry.backoff, BackoffStrategy::Exponential));
    }
}

fn value_to_string(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::SmallInt(i) => i.to_string(),
        Value::Int32(i) => i.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Uint(u) => u.to_string(),
        Value::Usize(u) => u.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Decimal(d) => d.to_string(),
        Value::Boolean(b) => b.to_string(),
        Value::Uuid(u) => u.to_string(),
        Value::Date(d) => d.to_string(),
        Value::Timestamp(t) => t.to_rfc3339(),
        Value::TimestampNaive(t) => t.to_string(),
        Value::Bytes(b) => String::from_utf8_lossy(b).to_string(),
        Value::Json(v) => v.to_string(),
        Value::Null => String::new(),
        Value::Enum(_, v) => v.clone(),
        Value::StringArray(v) => format!("{v:?}"),
    }
}
