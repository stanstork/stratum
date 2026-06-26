use crate::builder::analysis::AnalyzerError;
use connectors::error::{DbError, DriverError};
use engine_wasm::error::WasmError;
use std::time::Duration;
use thiserror::Error;

/// Top-level error for report building operations
#[derive(Error, Debug)]
pub enum ReportBuilderError {
    #[error("Connection error: {0}")]
    Connection(#[from] ConnectionError),

    #[error("Row counting error: {0}")]
    RowCount(#[from] RowCountError),

    #[error("Schema introspection error: {0}")]
    Schema(#[from] SchemaError),

    #[error("Filter analysis error: {0}")]
    Filter(#[from] FilterAnalyzerError),

    #[error("Join analysis error: {0}")]
    Join(#[from] JoinAnalyzerError),

    #[error("Mapping analysis error: {0}")]
    Mapping(#[from] MappingAnalyzerError),

    #[error("Validation analysis error: {0}")]
    Validation(#[from] ValidationAnalyzerError),

    #[error("Pagination analysis error: {0}")]
    Pagination(#[from] PaginationAnalyzerError),

    #[error("Hooks analysis error: {0}")]
    Hooks(#[from] HooksAnalyzerError),

    #[error("Schema analysis error: {0}")]
    SchemaAnalyzer(#[from] SchemaAnalyzerError),

    #[error("Sample collection error: {0}")]
    Sample(#[from] SampleCollectorError),

    #[error("Source analysis error: {0}")]
    SourceAnalyzer(#[from] SourceAnalyzerError),

    #[error("Timeout: {operation} exceeded {timeout:?}")]
    Timeout {
        operation: String,
        timeout: Duration,
    },

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Driver error: {0}")]
    DriverError(#[from] DriverError),

    #[error("Unsupported driver: {0}")]
    UnsupportedDriver(String),

    #[error("Invalid driver type: {0}")]
    InvalidDriverType(String),

    #[error("Plugin load failed: {0}")]
    Plugin(#[from] WasmError),
}

/// Connection-related errors
#[derive(Error, Debug)]
pub enum ConnectionError {
    #[error("Connection '{name}' not found in configuration")]
    NotFound { name: String },

    #[error("Failed to connect to '{name}': {reason}")]
    Failed { name: String, reason: String },

    #[error("Connection '{name}' timed out after {timeout_ms}ms")]
    Timeout { name: String, timeout_ms: u64 },

    #[error("Authentication failed for '{name}': {reason}")]
    AuthFailed { name: String, reason: String },

    #[error("SSL/TLS error for '{name}': {reason}")]
    SslError { name: String, reason: String },

    #[error("Driver error: {0}")]
    DriverError(#[from] DriverError),
}

/// Row counting errors
#[derive(Error, Debug)]
pub enum RowCountError {
    #[error("Query failed: {0}")]
    QueryFailed(String),

    #[error("Method not supported for this database driver")]
    NotSupported,

    #[error("Timeout getting row count for '{table}'")]
    Timeout { table: String },

    #[error("Table '{table}' not found")]
    TableNotFound { table: String },
}

/// Schema introspection errors
#[derive(Error, Debug)]
pub enum SchemaError {
    #[error("Failed to introspect table '{table}': {reason}")]
    IntrospectionFailed { table: String, reason: String },

    #[error("Table '{table}' not found in schema '{schema}'")]
    TableNotFound { table: String, schema: String },

    #[error("Column '{column}' not found in table '{table}'")]
    ColumnNotFound { column: String, table: String },

    #[error("Unsupported data type: {0}")]
    UnsupportedType(String),
}

/// Filter analysis errors
#[derive(Error, Debug)]
pub enum FilterAnalyzerError {
    #[error("Failed to parse filter expression: {0}")]
    ParseError(String),

    #[error("Invalid column reference '{column}' in filter")]
    InvalidColumn { column: String },

    #[error("Unsupported operator in filter: {0}")]
    UnsupportedOperator(String),

    #[error("Type mismatch in filter: expected {expected}, got {actual}")]
    TypeMismatch { expected: String, actual: String },

    #[error("Query failed during filter analysis: {0}")]
    QueryFailed(String),

    #[error("Unsupported source type for filtering: {source_type}")]
    UnsupportedSourceType { source_type: String },
}

/// Join analysis errors
#[derive(Error, Debug)]
pub enum JoinAnalyzerError {
    #[error("Connection '{name}' not found for join")]
    ConnectionNotFound { name: String },

    #[error("Failed to parse join condition: {0}")]
    ParseError(String),

    #[error("Invalid join: table '{table}' not found")]
    TableNotFound { table: String },

    #[error("Invalid join column: '{table}.{column}' not found")]
    ColumnNotFound { table: String, column: String },

    #[error("Cross-database join not supported between '{left}' and '{right}'")]
    CrossDatabaseNotSupported { left: String, right: String },

    #[error("Query failed during join analysis: {0}")]
    QueryFailed(String),

    #[error("Analyzer error: {0}")]
    Analyzer(#[from] AnalyzerError),
}

/// Mapping analysis errors
#[derive(Error, Debug)]
pub enum MappingAnalyzerError {
    #[error("Failed to parse mapping expression: {0}")]
    ParseError(String),

    #[error("Source column '{column}' not found")]
    SourceColumnNotFound { column: String },

    #[error("Target column '{column}' not found")]
    TargetColumnNotFound { column: String },

    #[error("Type conversion not supported: {from} -> {to}")]
    UnsupportedConversion { from: String, to: String },

    #[error("Circular reference detected in computed column '{column}'")]
    CircularReference { column: String },

    #[error("Function '{name}' not supported")]
    UnsupportedFunction { name: String },

    #[error("Query failed during mapping analysis: {0}")]
    QueryFailed(String),

    #[error("Source table '{table}' not found in metadata graph")]
    SourceTableNotFound { table: String },

    #[error("Unsupported expression in mapping: {0}")]
    UnsupportedExpression(String),
}

/// Validation analysis errors
#[derive(Error, Debug)]
pub enum ValidationAnalyzerError {
    #[error("Failed to parse validation expression: {0}")]
    ParseError(String),

    #[error("Invalid column reference: {0}")]
    InvalidColumnReference(String),

    #[error("Column '{column}' not found")]
    ColumnNotFound { column: String },

    #[error("Validation '{name}' has invalid action: {action}")]
    InvalidAction { name: String, action: String },

    #[error("Query failed during validation analysis: {0}")]
    QueryFailed(String),

    #[error("Analyzer error: {0}")]
    AnalysisError(String),

    #[error("Table '{0}' not found in metadata graph")]
    TableNotFound(String),
}

/// Pagination analysis errors
#[derive(Error, Debug)]
pub enum PaginationAnalyzerError {
    #[error("Unsupported pagination strategy: {strategy}")]
    UnsupportedStrategy { strategy: String },

    #[error("Invalid cursor column '{cursor}': {reason}")]
    InvalidCursor { cursor: String, reason: String },

    #[error("Cursor column '{column}' not found in table '{table}'")]
    CursorColumnNotFound { table: String, column: String },

    #[error("Failed to fetch metadata for table '{table}': {reason}")]
    MetadataError { table: String, reason: String },

    #[error("Timezone '{timezone}' is invalid")]
    InvalidTimezone { timezone: String },
}

/// Hooks analysis errors
#[derive(Error, Debug)]
pub enum HooksAnalyzerError {
    #[error("Invalid SQL statement: {sql}")]
    InvalidSql { sql: String },

    #[error("Hook analysis failed: {reason}")]
    AnalysisError { reason: String },
}

/// Schema analysis errors
#[derive(Error, Debug)]
pub enum SchemaAnalyzerError {
    #[error("Failed to analyze schema: {reason}")]
    AnalysisError { reason: String },

    #[error("Metadata error: {reason}")]
    MetadataError { reason: String },

    #[error("Incompatible schema change: {description}")]
    IncompatibleChange { description: String },

    #[error("Database error: {0}")]
    DatabaseError(#[from] DbError),
}

/// Sample collection errors
#[derive(Error, Debug)]
pub enum SampleCollectorError {
    #[error("Query failed while fetching samples: {0}")]
    QueryFailed(String),

    #[error("Failed to build transform pipeline: {0}")]
    PipelineBuildFailed(String),

    #[error("Transform failed for sample row {index}: {reason}")]
    TransformFailed { index: usize, reason: String },

    #[error("Validation execution failed: {0}")]
    ValidationFailed(String),

    #[error("Timeout collecting samples from '{table}'")]
    Timeout { table: String },

    #[error("No rows available for sampling in '{table}'")]
    NoRows { table: String },

    #[error("Unsupported source type for sampling: {source_type}")]
    UnsupportedSourceType { source_type: String },

    #[error("Unsupported sampling method: {method}")]
    UnsupportedSamplingMethod { method: String },

    #[error("Missing required configuration field '{field}' for sampling method '{method}'")]
    MissingRequiredConfig { field: String, method: String },

    #[error("Query execution failed for table '{table}': {error}")]
    QueryExecutionFailed { table: String, error: String },
}

/// Errors that can occur during source table analysis
#[derive(Error, Debug)]
pub enum SourceAnalyzerError {
    /// Table not found in source database
    #[error("Table '{table}' not found in source database")]
    TableNotFound { table: String },

    /// Column not found in source table
    #[error("Column '{column}' not found in table '{table}'")]
    ColumnNotFound { table: String, column: String },

    /// Schema introspection failed
    #[error("Failed to introspect schema for '{table}': {reason}")]
    IntrospectionFailed { table: String, reason: String },

    /// Database query failed
    #[error("Query failed: {0}")]
    QueryFailed(String),

    /// Connection not available
    #[error("Connection '{name}' not available for introspection")]
    ConnectionNotAvailable { name: String },

    /// Unsupported database type
    #[error("Unsupported database type '{driver}' for introspection")]
    UnsupportedDriver { driver: String },

    /// Timeout during introspection
    #[error("Introspection timeout after {seconds}s for table '{table}'")]
    Timeout { table: String, seconds: u64 },

    /// Type mapping error
    #[error("Cannot map source type '{source_type}' for column '{column}'")]
    UnsupportedType { column: String, source_type: String },

    /// Unsopported source type
    #[error("Unsupported source type '{source_type}' for analysis")]
    UnsupportedSourceType { source_type: String },

    /// Driver error
    #[error("Driver error: {0}")]
    DriverError(#[from] DriverError),
}

impl SourceAnalyzerError {
    /// Create a table not found error
    pub fn table_not_found(table: impl Into<String>) -> Self {
        Self::TableNotFound {
            table: table.into(),
        }
    }

    /// Create a column not found error
    pub fn column_not_found(table: impl Into<String>, column: impl Into<String>) -> Self {
        Self::ColumnNotFound {
            table: table.into(),
            column: column.into(),
        }
    }

    /// Check if this error is recoverable (plan can continue)
    pub fn is_recoverable(&self) -> bool {
        matches!(self, Self::Timeout { .. } | Self::UnsupportedType { .. })
    }
}

/// Result type alias for report builder operations
pub type ReportBuilderResult<T> = Result<T, ReportBuilderError>;
