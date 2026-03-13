//! Defines the AST for a CREATE INDEX statement.

use crate::ast::common::TableRef;

/// Represents a complete CREATE INDEX statement.
#[derive(Debug, Clone)]
pub struct CreateIndex {
    pub name: String,
    pub table: TableRef,
    pub columns: Vec<IndexColumnExpr>,
    pub unique: bool,
    pub if_not_exists: bool,
    pub concurrent: bool,
    pub index_type: Option<String>,
    pub condition: Option<String>,
}

/// A column or expression within an index definition.
#[derive(Debug, Clone)]
pub struct IndexColumnExpr {
    pub expr: String,
    pub sort_order: Option<String>,
    pub nulls: Option<String>,
}
