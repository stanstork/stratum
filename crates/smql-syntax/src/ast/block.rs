use crate::ast::{attribute::Attribute, pipeline::NestedBlock, span::Span};

/// Define block for constants/computed values
/// Syntax: define { tax_rate = 1.4, cutoff_date = "2024-01-01" }
#[derive(Debug, Clone, PartialEq)]
pub struct DefineBlock {
    pub attributes: Vec<Attribute>,
    pub span: Span,
}

/// Connection block for data sources
/// Syntax: connection "mysql_prod" { driver = "mysql", ... }
#[derive(Debug, Clone, PartialEq)]
pub struct ConnectionBlock {
    pub name: String,
    pub attributes: Vec<Attribute>,
    pub nested_blocks: Vec<NestedBlock>,
    pub span: Span,
}
