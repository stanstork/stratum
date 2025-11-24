use crate::query::ast::common::TypeName;

/// Represents a CREATE TYPE ... AS ENUM statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateEnum {
    pub name: TypeName,
    pub values: Vec<String>,
}
