use crate::context::env::get_env;
use model::{
    core::value::Value,
    execution::{
        connection::Connection,
        errors::ConvertError,
        execution_config::{ExecutionConfig, ExecutionStrategy, FailureStrategy},
        expr::{BinaryOp, CompiledExpression},
        pipeline::{
            BackoffStrategy, DataDestination, DataSource, ErrorHandling, FailedRowsAction,
            FailedRowsConfig, FailedRowsDestination, FileFormat, Filter, Join, LifecycleHooks,
            Pagination, Pipeline, RetryConfig, Transformation, ValidationAction, ValidationRule,
            ValidationSeverity, WriteMode,
        },
        properties::Properties,
    },
};
use smql_syntax::ast::{
    block::{ConnectionBlock, DefineBlock, ExecutionBlock},
    expr::{Expression, ExpressionKind},
    literal::Literal,
    operator::BinaryOperator,
    pipeline::{FromBlock, PipelineBlock, ToBlock},
    validation::ValidationKind,
};
use std::{collections::HashMap, str::FromStr};

// ============================================================
// Attribute Name Constants
// ============================================================

// Connection attributes
const ATTR_DRIVER: &str = "driver";

// Execution config attributes
const ATTR_STRATEGY: &str = "strategy";
const ATTR_MAX_CONCURRENCY: &str = "max_concurrency";
const ATTR_ON_FAILURE: &str = "on_failure";
const ATTR_PIPELINE_TIMEOUT: &str = "pipeline_timeout";
const ATTR_TOTAL_TIMEOUT: &str = "total_timeout";

// Pipeline attributes
const ATTR_CONNECTION: &str = "connection";
const ATTR_TABLE: &str = "table";
const ATTR_MODE: &str = "mode";
const ATTR_STRATEGY_PAGINATION: &str = "strategy";
const ATTR_CURSOR: &str = "cursor";
const ATTR_TIEBREAKER: &str = "tiebreaker";
const ATTR_TIMEZONE: &str = "timezone";
const ATTR_MAX_ATTEMPTS: &str = "max_attempts";
const ATTR_ACTION: &str = "action";
const ATTR_PATH: &str = "path";
const ATTR_FORMAT: &str = "format";
const ATTR_SCHEMA: &str = "schema";

// Nested block names
const BLOCK_TABLE: &str = "table";
const BLOCK_FILE: &str = "file";
const BLOCK_PIPELINE: &str = "pipeline";

// Keywords
const KEYWORD_CONNECTION: &str = "connection";
const KEYWORD_DEFINE: &str = "define";

// Default values
const DEFAULT_PAGINATION_STRATEGY: &str = "default";
const DEFAULT_CURSOR: &str = "id";

// Write modes
const MODE_INSERT: &str = "insert";
const MODE_UPDATE: &str = "update";
const MODE_UPSERT: &str = "upsert";
const MODE_REPLACE: &str = "replace";

// Validation actions
const ACTION_SKIP: &str = "skip";
const ACTION_FAIL: &str = "fail";
const ACTION_WARN: &str = "warn";
const ACTION_CONTINUE: &str = "continue";

// Failed rows actions
const FAILED_ACTION_LOG: &str = "log";
const FAILED_ACTION_SAVE_TO_TABLE: &str = "save_to_table";

// File formats
const FORMAT_JSON: &str = "json";
const FORMAT_CSV: &str = "csv";
const FORMAT_PARQUET: &str = "parquet";

// Error messages
const ERR_MISSING_DRIVER: &str = "Connection missing driver";
const ERR_MISSING_FROM: &str = "Pipeline missing 'from' block";
const ERR_MISSING_TO: &str = "Pipeline missing 'to' block";
const ERR_MISSING_TABLE: &str = "Missing 'table' attribute";
const ERR_INVALID_STRATEGY: &str =
    "Invalid execution strategy: '{}'. Must be 'sequential' or 'parallel'";
const ERR_STRATEGY_NOT_STRING: &str = "strategy must be a string";
const ERR_MAX_CONCURRENCY_NOT_NUMBER: &str = "max_concurrency must be a number";
const ERR_MAX_CONCURRENCY_RANGE: &str = "max_concurrency must be between 1 and 100";
const ERR_MAX_CONCURRENCY_REQUIRED: &str =
    "max_concurrency is required when strategy is 'parallel'";
const ERR_INVALID_FAILURE_STRATEGY: &str =
    "Invalid failure strategy: '{}'. Must be 'fail_fast' or 'continue'";
const ERR_ON_FAILURE_NOT_STRING: &str = "on_failure must be a string";
const ERR_TIMEOUT_NOT_STRING: &str = "{} must be a string (e.g., '30s', '5m', '2h')";
const ERR_MISSING_CONNECTION: &str = "From block missing connection attribute";
const ERR_MISSING_TO_CONNECTION: &str = "To block missing connection attribute";
const ERR_INVALID_PIPELINE_DEPENDENCY: &str =
    "Invalid pipeline dependency reference: {}. Expected format: pipeline.name";
const ERR_PIPELINE_DEPS_MUST_BE_STRINGS: &str =
    "Pipeline dependencies must be string literals or pipeline references";

// Validation constants
const MAX_CONCURRENCY_MIN: u32 = 1;
const MAX_CONCURRENCY_MAX: u32 = 100;

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
            let value = Self::eval_expression(&attr.value)?;
            properties.insert(attr.key.name.clone(), value);
        }

        for nested in &conn_block.nested_blocks {
            let mut nested_props = HashMap::new();
            for attr in &nested.attributes {
                let value = Self::eval_expression(&attr.value)?;
                nested_props.insert(attr.key.name.clone(), value);
            }
            nested_configs.insert(nested.kind.clone(), nested_props);
        }

        Ok(Connection {
            name: conn_block.name.clone(),
            driver: properties
                .get_string(ATTR_DRIVER)
                .ok_or_else(|| ConvertError::Connection(ERR_MISSING_DRIVER.to_string()))?,
            properties,
            nested_configs,
        })
    }

    pub fn build_execution_config(
        &self,
        exec_block: &ExecutionBlock,
    ) -> Result<ExecutionConfig, ConvertError> {
        let mut strategy = ExecutionStrategy::Sequential;
        let mut max_concurrency = None;
        let mut on_failure = FailureStrategy::FailFast;
        let mut pipeline_timeout = None;
        let mut total_timeout = None;

        for attr in &exec_block.attributes {
            let value = Self::eval_expression(&attr.value)?;

            match attr.key.name.as_str() {
                ATTR_STRATEGY => {
                    if let Value::String(s) = value {
                        strategy = ExecutionStrategy::from_str(&s).map_err(|_| {
                            ConvertError::Plan(ERR_INVALID_STRATEGY.replace("{}", &s))
                        })?;
                    } else {
                        return Err(ConvertError::Plan(ERR_STRATEGY_NOT_STRING.to_string()));
                    }
                }
                ATTR_MAX_CONCURRENCY => {
                    let concurrency = match value {
                        Value::Int32(n) => n as u32,
                        Value::Int(n) => n as u32,
                        Value::Uint(n) => n as u32,
                        Value::Float(f) => f as u32,
                        _ => {
                            return Err(ConvertError::Plan(
                                ERR_MAX_CONCURRENCY_NOT_NUMBER.to_string(),
                            ));
                        }
                    };

                    if !(MAX_CONCURRENCY_MIN..=MAX_CONCURRENCY_MAX).contains(&concurrency) {
                        return Err(ConvertError::Plan(ERR_MAX_CONCURRENCY_RANGE.to_string()));
                    }
                    max_concurrency = Some(concurrency);
                }
                ATTR_ON_FAILURE => {
                    if let Value::String(s) = value {
                        on_failure = FailureStrategy::from_str(&s).map_err(|_| {
                            ConvertError::Plan(ERR_INVALID_FAILURE_STRATEGY.replace("{}", &s))
                        })?;
                    } else {
                        return Err(ConvertError::Plan(ERR_ON_FAILURE_NOT_STRING.to_string()));
                    }
                }
                ATTR_PIPELINE_TIMEOUT => {
                    if let Value::String(s) = value {
                        pipeline_timeout = Some(parse_duration(&s)?);
                    } else {
                        return Err(ConvertError::Plan(
                            ERR_TIMEOUT_NOT_STRING.replace("{}", ATTR_PIPELINE_TIMEOUT),
                        ));
                    }
                }
                ATTR_TOTAL_TIMEOUT => {
                    if let Value::String(s) = value {
                        total_timeout = Some(parse_duration(&s)?);
                    } else {
                        return Err(ConvertError::Plan(
                            ERR_TIMEOUT_NOT_STRING.replace("{}", ATTR_TOTAL_TIMEOUT),
                        ));
                    }
                }
                _ => {
                    // Ignore unknown attributes for forward compatibility
                }
            }
        }

        // Validate that parallel strategy has max_concurrency
        if strategy == ExecutionStrategy::Parallel && max_concurrency.is_none() {
            return Err(ConvertError::Plan(ERR_MAX_CONCURRENCY_REQUIRED.to_string()));
        }

        Ok(ExecutionConfig {
            strategy,
            max_concurrency,
            on_failure,
            pipeline_timeout,
            total_timeout,
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
            .ok_or_else(|| ConvertError::Plan(ERR_MISSING_FROM.to_string()))?;

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
                .find(|a| a.key.name == ATTR_STRATEGY_PAGINATION)
                .and_then(|a| Self::eval_expression(&a.value).ok())
                .and_then(|v| match v {
                    Value::String(s) => Some(s),
                    _ => None,
                })
                .unwrap_or_else(|| DEFAULT_PAGINATION_STRATEGY.to_string());

            let cursor = p
                .attributes
                .iter()
                .find(|a| a.key.name == ATTR_CURSOR)
                .and_then(|a| Self::eval_expression(&a.value).ok())
                .and_then(|v| match v {
                    Value::String(s) => Some(s),
                    _ => None,
                })
                .unwrap_or_else(|| DEFAULT_CURSOR.to_string());

            let tiebreaker = p
                .attributes
                .iter()
                .find(|a| a.key.name == ATTR_TIEBREAKER)
                .and_then(|a| Self::eval_expression(&a.value).ok())
                .and_then(|v| match v {
                    Value::String(s) => Some(s),
                    _ => None,
                });

            let timezone = p
                .attributes
                .iter()
                .find(|a| a.key.name == ATTR_TIMEZONE)
                .and_then(|a| Self::eval_expression(&a.value).ok())
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
            .find(|a| a.key.name == ATTR_TABLE)
            .and_then(|a| Self::eval_expression(&a.value).ok())
            .and_then(|v| match v {
                Value::String(s) => Some(s),
                _ => None,
            })
            .ok_or_else(|| ConvertError::Plan(ERR_MISSING_TABLE.to_string()))?;

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
            .ok_or_else(|| ConvertError::Plan(ERR_MISSING_TO.to_string()))?;

        let connection = self.resolve_connection_to(to)?;

        // Extract table name
        let table = to
            .attributes
            .iter()
            .find(|a| a.key.name == ATTR_TABLE)
            .and_then(|a| Self::eval_expression(&a.value).ok())
            .and_then(|v| match v {
                Value::String(s) => Some(s),
                _ => None,
            })
            .ok_or_else(|| ConvertError::Plan(ERR_MISSING_TABLE.to_string()))?;

        let mode = to
            .attributes
            .iter()
            .find(|a| a.key.name == ATTR_MODE)
            .and_then(|a| Self::eval_expression(&a.value).ok())
            .and_then(|v| match v {
                Value::String(s) => match s.as_str() {
                    MODE_INSERT => Some(WriteMode::Insert),
                    MODE_UPDATE => Some(WriteMode::Update),
                    MODE_UPSERT => Some(WriteMode::Upsert),
                    MODE_REPLACE => Some(WriteMode::Replace),
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
        // The 'after' field is Vec<Expression>, where each expression is typically an array
        if let Some(after) = &pipeline_block.after {
            let mut deps = Vec::new();
            for expr in after {
                match &expr.kind {
                    // Handle array of dependencies: after = [pipeline.name1, pipeline.name2]
                    ExpressionKind::Array(items) => {
                        for item in items {
                            match &item.kind {
                                // String literal in array: after = ["pipeline1"]
                                ExpressionKind::Literal(Literal::String(s)) => {
                                    deps.push(s.clone());
                                }
                                // Dot notation in array: after = [pipeline.copy_actors]
                                ExpressionKind::DotNotation(dot_path) => {
                                    if dot_path.segments.len() == 2
                                        && dot_path.segments[0] == BLOCK_PIPELINE
                                    {
                                        deps.push(dot_path.segments[1].clone());
                                    } else {
                                        return Err(ConvertError::Plan(
                                            ERR_INVALID_PIPELINE_DEPENDENCY
                                                .replace("{}", &dot_path.segments.join(".")),
                                        ));
                                    }
                                }
                                _ => {
                                    return Err(ConvertError::Plan(
                                        ERR_PIPELINE_DEPS_MUST_BE_STRINGS.to_string(),
                                    ));
                                }
                            }
                        }
                    }
                    // Handle single string literal: after = "pipeline1" (legacy/edge case)
                    ExpressionKind::Literal(Literal::String(s)) => {
                        deps.push(s.clone());
                    }
                    // Handle single dot notation: after = pipeline.name (legacy/edge case)
                    ExpressionKind::DotNotation(dot_path) => {
                        if dot_path.segments.len() == 2 && dot_path.segments[0] == BLOCK_PIPELINE {
                            deps.push(dot_path.segments[1].clone());
                        } else {
                            return Err(ConvertError::Plan(
                                ERR_INVALID_PIPELINE_DEPENDENCY
                                    .replace("{}", &dot_path.segments.join(".")),
                            ));
                        }
                    }
                    _ => {
                        return Err(ConvertError::Plan(
                            ERR_PIPELINE_DEPS_MUST_BE_STRINGS.to_string(),
                        ));
                    }
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
                            ACTION_SKIP => Some(ValidationAction::Skip),
                            ACTION_FAIL => Some(ValidationAction::Fail),
                            ACTION_WARN => Some(ValidationAction::Warn),
                            ACTION_CONTINUE => Some(ValidationAction::Continue),
                            _ => None,
                        })
                        .unwrap_or(ValidationAction::Warn),
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
                    .find(|a| a.key.name == ATTR_MAX_ATTEMPTS)
                    .and_then(|a| Self::eval_expression(&a.value).ok())
                    .and_then(|v| match v {
                        Value::Int32(n) => Some(n as u32),
                        Value::Float(f) => Some(f as u32),
                        _ => None,
                    })
                    .unwrap_or(3);

                RetryConfig {
                    max_attempts,
                    delay_ms: 1000,
                    backoff: BackoffStrategy::Exponential,
                }
            });

            let failed_rows = on_error.failed_rows.as_ref().map(|fr| {
                // Extract action from attributes (optional, defaults to Log)
                let action = fr
                    .attributes
                    .iter()
                    .find(|a| a.key.name == ATTR_ACTION)
                    .and_then(|a| Self::eval_expression(&a.value).ok())
                    .and_then(|v| match v {
                        Value::String(s) => match s.as_str() {
                            ACTION_SKIP => Some(FailedRowsAction::Skip),
                            FAILED_ACTION_LOG => Some(FailedRowsAction::Log),
                            FAILED_ACTION_SAVE_TO_TABLE => Some(FailedRowsAction::SaveToTable),
                            _ => None,
                        },
                        _ => None,
                    })
                    .unwrap_or(FailedRowsAction::Log);

                // Extract destination from nested blocks or attributes
                let destination = if let Some(table_block) =
                    fr.nested_blocks.iter().find(|b| b.kind == BLOCK_TABLE)
                {
                    // Parse table block
                    let connection_name = table_block
                        .attributes
                        .iter()
                        .find(|a| a.key.name == ATTR_CONNECTION)
                        .and_then(|a| {
                            if let ExpressionKind::DotNotation(path) = &a.value.kind {
                                // connection.name format
                                if path.segments.len() == 2
                                    && path.segments[0] == KEYWORD_CONNECTION
                                {
                                    Some(path.segments[1].clone())
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        });

                    let schema = table_block
                        .attributes
                        .iter()
                        .find(|a| a.key.name == ATTR_SCHEMA)
                        .and_then(|a| Self::eval_expression(&a.value).ok())
                        .and_then(|v| match v {
                            Value::String(s) => Some(s),
                            _ => None,
                        });

                    let table = table_block
                        .attributes
                        .iter()
                        .find(|a| a.key.name == ATTR_TABLE)
                        .and_then(|a| Self::eval_expression(&a.value).ok())
                        .and_then(|v| match v {
                            Value::String(s) => Some(s),
                            _ => None,
                        });

                    if let (Some(conn_name), Some(tbl)) = (connection_name, table) {
                        // Look up the connection from the connections map
                        self.connections.get(&conn_name).map(|connection| {
                            FailedRowsDestination::Table {
                                connection: connection.clone(),
                                table: tbl,
                                schema,
                            }
                        })
                    } else {
                        None
                    }
                } else if let Some(file_block) =
                    fr.nested_blocks.iter().find(|b| b.kind == BLOCK_FILE)
                {
                    // Parse file block
                    let path = file_block
                        .attributes
                        .iter()
                        .find(|a| a.key.name == ATTR_PATH)
                        .and_then(|a| Self::eval_expression(&a.value).ok())
                        .and_then(|v| match v {
                            Value::String(s) => Some(s),
                            _ => None,
                        });

                    let format = file_block
                        .attributes
                        .iter()
                        .find(|a| a.key.name == ATTR_FORMAT)
                        .and_then(|a| Self::eval_expression(&a.value).ok())
                        .and_then(|v| match v {
                            Value::String(s) => match s.as_str() {
                                FORMAT_JSON => Some(FileFormat::Json),
                                FORMAT_CSV => Some(FileFormat::Csv),
                                FORMAT_PARQUET => Some(FileFormat::Parquet),
                                _ => None,
                            },
                            _ => None,
                        });

                    path.map(|p| FailedRowsDestination::File {
                        path: p.clone(),
                        format: format.unwrap_or_else(|| {
                            // Auto-detect format from extension if not specified
                            let ext_json = format!(".{}", FORMAT_JSON);
                            let ext_csv = format!(".{}", FORMAT_CSV);
                            let ext_parquet = format!(".{}", FORMAT_PARQUET);

                            if p.ends_with(&ext_json) {
                                FileFormat::Json
                            } else if p.ends_with(&ext_csv) {
                                FileFormat::Csv
                            } else if p.ends_with(&ext_parquet) {
                                FileFormat::Parquet
                            } else {
                                FileFormat::Json
                            }
                        }),
                    })
                } else {
                    None
                };

                FailedRowsConfig {
                    action,
                    destination,
                }
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
                let value = Self::eval_expression(&attr.value)?;
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
            if attr.key.name == ATTR_CONNECTION
                && let ExpressionKind::DotNotation(path) = &attr.value.kind
                && path.segments.len() == 2
                && path.segments[0] == KEYWORD_CONNECTION
            {
                return Ok(path.segments[1].clone());
            }
        }
        Err(ConvertError::Plan(ERR_MISSING_CONNECTION.to_string()))
    }

    fn resolve_connection_to(&self, to: &ToBlock) -> Result<String, ConvertError> {
        // Look for connection = connection.name attribute
        for attr in &to.attributes {
            if attr.key.name == ATTR_CONNECTION
                && let ExpressionKind::DotNotation(path) = &attr.value.kind
                && path.segments.len() == 2
                && path.segments[0] == KEYWORD_CONNECTION
            {
                return Ok(path.segments[1].clone());
            }
        }
        Err(ConvertError::Plan(ERR_MISSING_TO_CONNECTION.to_string()))
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
                if path.segments[0] == KEYWORD_DEFINE
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

    // Used for simple expressions only during plan building
    fn eval_expression(expr: &Expression) -> Result<Value, ConvertError> {
        let definitions = HashMap::new();
        expression_engine::eval_ast_expression(expr, &definitions, get_env)
            .map_err(|e| ConvertError::Expression(e.to_string()))
    }

    pub fn extract_definitions(
        &self,
        def_block: &DefineBlock,
    ) -> Result<HashMap<String, Value>, ConvertError> {
        let mut definitions = HashMap::new();
        for attr in &def_block.attributes {
            let value = Self::eval_expression(&attr.value)?;
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

/// Parse duration string (e.g., "30s", "5m", "2h") to seconds
fn parse_duration(s: &str) -> Result<u64, ConvertError> {
    let s = s.trim();
    if s.is_empty() {
        return Err(ConvertError::Plan("Empty duration string".to_string()));
    }

    let (num_str, unit) = if let Some(num_str) = s.strip_suffix("ms") {
        (num_str, "ms")
    } else if s.len() > 1 {
        (&s[..s.len() - 1], &s[s.len() - 1..])
    } else {
        return Err(ConvertError::Plan(format!(
            "Invalid duration format: '{}'. Expected format like '30s', '5m', '2h'",
            s
        )));
    };

    let num: u64 = num_str.parse().map_err(|_| {
        ConvertError::Plan(format!(
            "Invalid number in duration: '{}'. Expected format like '30s', '5m', '2h'",
            s
        ))
    })?;

    let seconds = match unit {
        "ms" => num / 1000, // milliseconds to seconds
        "s" => num,
        "m" => num * 60,
        "h" => num * 3600,
        "d" => num * 86400,
        _ => {
            return Err(ConvertError::Plan(format!(
                "Invalid duration unit: '{}'. Supported units: ms, s, m, h, d",
                unit
            )));
        }
    };

    Ok(seconds)
}

#[cfg(test)]
mod tests {
    use super::*;
    use smql_syntax::ast::{
        attribute::Attribute,
        dotpath::DotPath,
        ident::Identifier,
        pipeline::{AfterBlock, BeforeBlock, NestedBlock, PaginateBlock, SettingsBlock},
        span::Span,
        validation::{
            FailedRowsBlock, OnErrorBlock, RetryBlock, ValidateBlock, ValidationBody,
            ValidationCheck,
        },
    };
    use std::sync::Mutex;

    // Shared lock for tests that use the global EnvContext to prevent race conditions
    static ENV_TEST_LOCK: Mutex<()> = Mutex::new(());

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

    fn make_nested_block(kind: &str, attributes: Vec<Attribute>) -> NestedBlock {
        NestedBlock {
            kind: kind.to_string(),
            attributes,
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

        // Test with array of DotPath (modern syntax: after = [pipeline.name1, pipeline.name2])
        let array_expr = Expression::new(
            ExpressionKind::Array(vec![
                make_dotpath_expr(vec!["pipeline", "pipeline1"]),
                make_dotpath_expr(vec!["pipeline", "pipeline0"]),
            ]),
            test_span(),
        );

        let pipeline = PipelineBlock {
            name: "pipeline2".to_string(),
            description: None,
            after: Some(vec![array_expr]),
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

    #[test]
    fn test_failed_rows_with_table_block_with_schema() {
        let mut builder = PlanBuilder::new();

        // Add a test connection to the builder's connections map
        builder.connections.insert(
            "warehouse".to_string(),
            Connection {
                name: "warehouse".to_string(),
                driver: "postgres".to_string(),
                properties: Properties::new(),
                nested_configs: HashMap::new(),
            },
        );

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
                retry: None,
                failed_rows: Some(FailedRowsBlock {
                    attributes: vec![make_attribute("action", make_string_expr("save_to_table"))],
                    nested_blocks: vec![make_nested_block(
                        "table",
                        vec![
                            make_attribute(
                                "connection",
                                make_dotpath_expr(vec!["connection", "warehouse"]),
                            ),
                            make_attribute("schema", make_string_expr("dlq")),
                            make_attribute("table", make_string_expr("failed_orders")),
                        ],
                    )],
                    span: test_span(),
                }),
                span: test_span(),
            }),
            paginate_block: None,
            before_block: None,
            after_block: None,
            settings_block: None,
            span: test_span(),
        };

        let error_handling = builder.build_error_handling(&pipeline).unwrap();
        let failed_rows = error_handling.failed_rows.unwrap();

        assert!(matches!(failed_rows.action, FailedRowsAction::SaveToTable));

        match failed_rows.destination.unwrap() {
            FailedRowsDestination::Table {
                connection,
                table,
                schema,
            } => {
                assert_eq!(connection.name, "warehouse");
                assert_eq!(connection.driver, "postgres");
                assert_eq!(table, "failed_orders");
                assert_eq!(schema, Some("dlq".to_string()));
            }
            _ => panic!("Expected Table destination"),
        }
    }

    #[test]
    fn test_failed_rows_with_table_block_without_schema() {
        let mut builder = PlanBuilder::new();

        // Add a test connection to the builder's connections map
        builder.connections.insert(
            "error_db".to_string(),
            Connection {
                name: "error_db".to_string(),
                driver: "mysql".to_string(),
                properties: Properties::new(),
                nested_configs: HashMap::new(),
            },
        );

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
                retry: None,
                failed_rows: Some(FailedRowsBlock {
                    attributes: vec![],
                    nested_blocks: vec![make_nested_block(
                        "table",
                        vec![
                            make_attribute(
                                "connection",
                                make_dotpath_expr(vec!["connection", "error_db"]),
                            ),
                            make_attribute("table", make_string_expr("failed_rows")),
                        ],
                    )],
                    span: test_span(),
                }),
                span: test_span(),
            }),
            paginate_block: None,
            before_block: None,
            after_block: None,
            settings_block: None,
            span: test_span(),
        };

        let error_handling = builder.build_error_handling(&pipeline).unwrap();
        let failed_rows = error_handling.failed_rows.unwrap();

        // Should default to Log when no action specified
        assert!(matches!(failed_rows.action, FailedRowsAction::Log));

        match failed_rows.destination.unwrap() {
            FailedRowsDestination::Table {
                connection,
                table,
                schema,
            } => {
                assert_eq!(connection.name, "error_db");
                assert_eq!(connection.driver, "mysql");
                assert_eq!(table, "failed_rows");
                assert_eq!(schema, None);
            }
            _ => panic!("Expected Table destination"),
        }
    }

    #[test]
    fn test_failed_rows_with_file_block_explicit_format() {
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
                retry: None,
                failed_rows: Some(FailedRowsBlock {
                    attributes: vec![make_attribute("action", make_string_expr("log"))],
                    nested_blocks: vec![make_nested_block(
                        "file",
                        vec![
                            make_attribute("path", make_string_expr("/data/errors.csv")),
                            make_attribute("format", make_string_expr("csv")),
                        ],
                    )],
                    span: test_span(),
                }),
                span: test_span(),
            }),
            paginate_block: None,
            before_block: None,
            after_block: None,
            settings_block: None,
            span: test_span(),
        };

        let error_handling = builder.build_error_handling(&pipeline).unwrap();
        let failed_rows = error_handling.failed_rows.unwrap();

        assert!(matches!(failed_rows.action, FailedRowsAction::Log));

        match failed_rows.destination.unwrap() {
            FailedRowsDestination::File { path, format } => {
                assert_eq!(path, "/data/errors.csv");
                assert!(matches!(format, FileFormat::Csv));
            }
            _ => panic!("Expected File destination"),
        }
    }

    #[test]
    fn test_failed_rows_with_file_block_auto_detect_format() {
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
                retry: None,
                failed_rows: Some(FailedRowsBlock {
                    attributes: vec![],
                    nested_blocks: vec![make_nested_block(
                        "file",
                        vec![make_attribute(
                            "path",
                            make_string_expr("/logs/failed_rows.parquet"),
                        )],
                    )],
                    span: test_span(),
                }),
                span: test_span(),
            }),
            paginate_block: None,
            before_block: None,
            after_block: None,
            settings_block: None,
            span: test_span(),
        };

        let error_handling = builder.build_error_handling(&pipeline).unwrap();
        let failed_rows = error_handling.failed_rows.unwrap();

        match failed_rows.destination.unwrap() {
            FailedRowsDestination::File { path, format } => {
                assert_eq!(path, "/logs/failed_rows.parquet");
                // Should auto-detect parquet from extension
                assert!(matches!(format, FileFormat::Parquet));
            }
            _ => panic!("Expected File destination"),
        }
    }

    #[test]
    fn test_env_function_with_typed_defaults() {
        use crate::context::env::{EnvContext, clear_env_context, init_env_context};

        // Use a static mutex to ensure tests using global env context don't run in parallel
        let _guard = ENV_TEST_LOCK.lock().unwrap();

        // Clean up and create fresh context
        clear_env_context();
        let mut env_ctx = EnvContext::empty();
        env_ctx.set("BATCH_SIZE".to_string(), "5000".to_string());
        env_ctx.set("CREATE_TABLES".to_string(), "true".to_string());
        env_ctx.set("THRESHOLD".to_string(), "0.95".to_string());
        init_env_context(env_ctx);

        // Test integer default - env var exists
        let expr_int = Expression::new(
            ExpressionKind::FunctionCall {
                name: "env".to_string(),
                arguments: vec![
                    make_string_expr("BATCH_SIZE"),
                    make_number_expr(1000.0), // default
                ],
            },
            test_span(),
        );
        let result_int = PlanBuilder::eval_expression(&expr_int).unwrap();
        // Should parse as Uint since positive integer
        assert!(matches!(result_int, Value::Uint(5000)));

        // Test boolean default - env var exists
        let expr_bool = Expression::new(
            ExpressionKind::FunctionCall {
                name: "env".to_string(),
                arguments: vec![
                    make_string_expr("CREATE_TABLES"),
                    make_bool_expr(false), // default
                ],
            },
            test_span(),
        );
        let result_bool = PlanBuilder::eval_expression(&expr_bool).unwrap();
        assert!(matches!(result_bool, Value::Boolean(true)));

        // Test float default - env var exists
        let expr_float = Expression::new(
            ExpressionKind::FunctionCall {
                name: "env".to_string(),
                arguments: vec![
                    make_string_expr("THRESHOLD"),
                    make_number_expr(0.5), // default
                ],
            },
            test_span(),
        );
        let result_float = PlanBuilder::eval_expression(&expr_float).unwrap();
        assert!(matches!(result_float, Value::Float(v) if (v - 0.95).abs() < 0.001));

        // Test when env var doesn't exist - should return typed default
        let expr_missing_int = Expression::new(
            ExpressionKind::FunctionCall {
                name: "env".to_string(),
                arguments: vec![
                    make_string_expr("MISSING_VAR"),
                    make_number_expr(1234.0), // default
                ],
            },
            test_span(),
        );
        let result_missing = PlanBuilder::eval_expression(&expr_missing_int).unwrap();
        assert!(matches!(result_missing, Value::Float(1234.0)));

        clear_env_context();
    }

    #[test]
    fn test_env_function_parse_failure() {
        use crate::context::env::{EnvContext, clear_env_context, init_env_context};

        // Use a static mutex to ensure tests using global env context don't run in parallel
        let _guard = ENV_TEST_LOCK.lock().unwrap();

        clear_env_context();
        let mut env_ctx = EnvContext::empty();
        env_ctx.set("BAD_INT".to_string(), "not_a_number".to_string());
        init_env_context(env_ctx);

        // Try to parse invalid integer
        let expr = Expression::new(
            ExpressionKind::FunctionCall {
                name: "env".to_string(),
                arguments: vec![make_string_expr("BAD_INT"), make_number_expr(100.0)],
            },
            test_span(),
        );

        let result = PlanBuilder::eval_expression(&expr);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Failed to parse environment variable")
        );

        clear_env_context();
    }
}
