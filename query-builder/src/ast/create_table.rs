//! Defines the AST for a CREATE TABLE statement.

use crate::ast::{common::TableRef, expr::Expr};

/// Represents a complete CREATE TABLE statement.
#[derive(Debug, Clone, Default)]
pub struct CreateTable {
    pub table: TableRef,
    pub columns: Vec<ColumnDef>,
    pub constraints: Vec<TableConstraint>,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub is_nullable: bool,
    pub is_primary_key: bool,
    pub default_value: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TableConstraint {
    PrimaryKey {
        columns: Vec<String>,
    },
    ForeignKey {
        columns: Vec<String>,
        references: TableRef,
        referenced_columns: Vec<String>,
    },
}

/// A simplified representation of common SQL data types.
/// The renderer is responsible for converting these into dialect-specific strings
/// (e.g., `Serial` -> `SERIAL` in Postgres, `INTEGER AUTO_INCREMENT` in MySQL).
#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    Text,
    Integer,
    BigInteger,
    Serial, // Auto-incrementing integer
    BigSerial,
    Boolean,
    Geometry,
    Timestamp,
    Jsonb,
    Varchar(u32),
}
