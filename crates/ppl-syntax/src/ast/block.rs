use crate::ast::{attribute::Attribute, pipeline::NestedBlock, span::Span};
use serde::{Deserialize, Serialize};

/// Define block for constants/computed values
/// Syntax: define { tax_rate = 1.4, cutoff_date = "2024-01-01" }
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DefineBlock {
    pub attributes: Vec<Attribute>,
    pub span: Span,
}

/// Execution block for DAG execution configuration
/// Syntax: execution { strategy = "parallel", max_concurrency = 8, on_failure = "continue" }
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionBlock {
    pub attributes: Vec<Attribute>,
    pub span: Span,
}

/// Connection block for data sources
/// Syntax: connection "mysql_prod" { driver = "mysql", ... }
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConnectionBlock {
    pub name: String,
    pub attributes: Vec<Attribute>,
    pub nested_blocks: Vec<NestedBlock>,
    pub span: Span,
}

// Plugin block for WASM plugin definitions
// Syntax: plugin "my_plugin" { path = "./plugins/my_plugin.wasm", allow_http = true, ... }
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PluginBlock {
    pub name: String,
    pub attributes: Vec<Attribute>,
    pub nested_blocks: Vec<NestedBlock>,
    pub span: Span,
}
