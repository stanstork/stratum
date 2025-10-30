//! Defines the AST for an INSERT statement.

use crate::query::ast::{common::TableRef, expr::Expr};

/// Represents a complete INSERT statement.
///
/// This structure supports both single-row and multi-row (batch) inserts
/// through the `values` field, which is a list of rows.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Insert {
    pub table: TableRef,
    pub columns: Vec<String>,
    /// The rows of values to be inserted. Each inner vector represents a single row.
    pub values: Vec<Vec<Expr>>,
}
