//! Defines the AST for ALTER TABLE statements.

use crate::ast::{
    common::TableRef,
    create_table::{ColumnDef, TableConstraint},
};

/// Represents a complete ALTER TABLE statement, which can contain multiple operations.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct AlterTable {
    pub table: TableRef,
    pub operations: Vec<AlterTableOperation>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterTableOperation {
    AddColumn(ColumnDef),
    AddConstraint(TableConstraint),
    ToggleTriggers { enabled: bool },
}
