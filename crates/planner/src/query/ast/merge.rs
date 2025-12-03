//! Defines the AST for SQL MERGE statements.

use crate::query::ast::{common::TableRef, expr::Expr};

#[derive(Debug, Clone)]
pub struct Merge {
    pub target: TableRef,
    pub target_alias: Option<String>,
    pub source: TableRef,
    pub source_alias: Option<String>,
    pub on: Expr,
    pub when_matched: Option<MergeMatched>,
    pub when_not_matched: Option<MergeNotMatched>,
}

#[derive(Debug, Clone)]
pub enum MergeMatched {
    Update { assignments: Vec<MergeAssignment> },
    DoNothing,
}

#[derive(Debug, Clone)]
pub struct MergeNotMatched {
    pub columns: Vec<String>,
    pub values: Vec<Expr>,
}

#[derive(Debug, Clone)]
pub struct MergeAssignment {
    pub column: String,
    pub value: Expr,
}
