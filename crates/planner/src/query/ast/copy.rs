//! Defines the AST for SQL COPY statements.

use crate::query::ast::common::TableRef;

#[derive(Debug, Clone)]
pub struct Copy {
    pub table: TableRef,
    pub columns: Vec<String>,
    pub direction: CopyDirection,
    pub endpoint: CopyEndpoint,
    pub options: Vec<CopyOption>,
}

#[derive(Debug, Clone)]
pub enum CopyDirection {
    From,
    To,
}

#[derive(Debug, Clone)]
pub enum CopyEndpoint {
    Stdin,
    Stdout,
    File(String),
    Program(String),
}

#[derive(Debug, Clone)]
pub struct CopyOption {
    pub key: String,
    pub value: Option<String>,
}
