//! Defines the AST for an INSERT statement.

use crate::query::ast::{common::TableRef, expr::Expr, select::Select};

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
    /// Optional SELECT query used as the data source.
    pub select: Option<Select>,
    /// Optional ON CONFLICT clause for handling conflicts.
    pub on_conflict: Option<OnConflict>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OnConflict {
    pub columns: Vec<String>,
    pub action: ConflictAction,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ConflictAction {
    DoNothing,
    DoUpdate {
        assignments: Vec<ConflictAssignment>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConflictAssignment {
    pub column: String,
    pub value: Expr,
}
