//! Defines the AST for SQL expressions.

use common::value::Value;

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// A column or table identifier, e.g., `users` or `users.id`.
    Identifier(Ident),

    /// A literal value, such as a string, number, boolean, or NULL.
    Value(Value),

    /// A binary operation, e.g., `column = 'value'` or `a + b`.
    BinaryOp(Box<BinaryOp>),

    /// A function call, e.g., `COUNT(*)` or `MAX(price)`.
    FunctionCall(FunctionCall),

    /// An aliased expression, e.g. `COUNT(*) AS total_count`
    Alias { expr: Box<Expr>, alias: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ident {
    pub qualifier: Option<String>, // e.g., the 'users' in 'users.id'
    pub name: String,              // e.g., the 'id' in 'users.id'
}

#[derive(Debug, Clone, PartialEq)]
pub struct BinaryOp {
    pub left: Expr,
    pub op: BinaryOperator,
    pub right: Expr,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FunctionCall {
    pub name: String,
    pub args: Vec<Expr>,
    pub wildcard: bool, // represents the '*' in 'COUNT(*)'
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BinaryOperator {
    // Comparison
    Eq,    // =
    NotEq, // <>
    Lt,    // <
    LtEq,  // <=
    Gt,    // >
    GtEq,  // >=

    // Logical
    And,
    Or,
}
