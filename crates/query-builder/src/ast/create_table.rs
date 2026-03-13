//! Defines the AST for a CREATE TABLE statement.

use crate::ast::{common::TableRef, expr::Expr};
use model::core::types::Type;

/// Represents a complete CREATE TABLE statement.
#[derive(Debug, Clone, Default)]
pub struct CreateTable {
    pub table: TableRef,
    pub columns: Vec<ColumnDef>,
    pub constraints: Vec<TableConstraint>,
    pub if_not_exists: bool,
    pub temp: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: Type,
    pub is_nullable: bool,
    pub is_primary_key: bool,
    pub default_value: Option<Expr>,
    pub max_length: Option<usize>,
    pub generated_expression: Option<String>,
    pub is_stored: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TableConstraint {
    PrimaryKey {
        columns: Vec<String>,
    },
    ForeignKey {
        name: Option<String>,
        columns: Vec<String>,
        references: TableRef,
        referenced_columns: Vec<String>,
        on_delete: Option<String>,
        on_update: Option<String>,
    },
    Unique {
        name: Option<String>,
        columns: Vec<String>,
    },
    Check {
        name: Option<String>,
        expression: String,
    },
}
