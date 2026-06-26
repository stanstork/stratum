use crate::{
    core::value::Value,
    execution::{connection::Connection, expr::CompiledExpression, references::GraphReferences},
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Pipeline block compiled to execution instructions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pipeline {
    pub name: String,
    pub description: Option<String>,
    pub dependencies: Vec<String>,
    pub source: DataSource,
    pub destination: DataDestination,
    pub transformations: Vec<Transformation>,
    /// Field mappings for referenced (cascade) tables keyed by source table name.
    pub named_transformations: HashMap<String, Vec<Transformation>>,
    pub validations: Vec<ValidationRule>,
    pub lifecycle: Option<LifecycleHooks>,
    pub error_handling: Option<ErrorHandling>,
    pub settings: HashMap<String, Value>,
    pub plugin_transforms: Vec<PluginTransformCall>,
}

/// From block - data source configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataSource {
    pub connection: Connection,
    pub table: String,
    pub filters: Vec<Filter>,
    pub joins: Vec<Join>,
    pub pagination: Option<Pagination>,
    pub graph_references: Option<GraphReferences>,
}

/// To block - data destination configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataDestination {
    pub connection: Connection,
    pub table: String,
    pub mode: WriteMode,
    pub table_map: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WriteMode {
    Insert,
    Update,
    Upsert,
    Replace,
}

/// Where clause filter
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Filter {
    pub label: Option<String>,
    pub condition: CompiledExpression,
}

/// With block join
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Join {
    pub alias: String,
    pub table: String,
    pub condition: Option<CompiledExpression>,
}

/// Paginate block configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pagination {
    pub strategy: String,
    pub column: String,
    pub tiebreaker: Option<String>,
    pub timezone: Option<String>,
}

/// Select block field mapping
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transformation {
    pub target_field: String,
    pub expression: CompiledExpression,
}

/// Validate block rule. Common metadata lives at the top level; the per-variant
/// payload (assert expression vs. WASM filter call) lives in `kind`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationRule {
    pub label: String,
    pub severity: ValidationSeverity,
    pub message: String,
    pub action: ValidationAction,
    pub kind: ValidationKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ValidationKind {
    /// Expression-based assertion (existing behavior).
    Assert { check: CompiledExpression },
    /// Delegates the pass/reject decision to a WASM filter plugin.
    WasmFilter {
        plugin_name: String,
        input_mapping: HashMap<String, String>,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ValidationSeverity {
    Assert,
    Warn,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ValidationAction {
    Skip,
    Fail,
    Warn,
    Continue,
}

/// Before/after blocks - lifecycle hooks
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleHooks {
    pub before: Vec<String>,
    pub after: Vec<String>,
}

/// On_error block configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorHandling {
    pub retry: Option<RetryConfig>,
    pub failed_rows: Option<FailedRowsConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    pub max_attempts: u32,
    pub delay_ms: u64,
    pub backoff: BackoffStrategy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BackoffStrategy {
    Fixed,
    Exponential,
    Linear,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailedRowsConfig {
    pub action: FailedRowsAction,
    pub destination: Option<FailedRowsDestination>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FailedRowsAction {
    Skip,
    Log,
    SaveToTable,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FailedRowsDestination {
    Table {
        connection: Connection,
        table: String,
        schema: Option<String>,
    },
    File {
        path: String,
        format: FileFormat,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FileFormat {
    Json,
    Csv,
    Parquet,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginTransformCall {
    pub plugin_name: String,
    pub output_column: String,
    pub input_mapping: HashMap<String, String>,
}
