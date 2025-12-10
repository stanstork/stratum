use crate::core::value::Value;
use serde::{Deserialize, Serialize};

/// Compiled expression ready for runtime evaluation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CompiledExpression {
    Literal(Value),
    Identifier(String),
    DotPath(Vec<String>),
    Binary {
        left: Box<CompiledExpression>,
        op: BinaryOp,
        right: Box<CompiledExpression>,
    },
    Unary {
        op: UnaryOp,
        operand: Box<CompiledExpression>,
    },
    FunctionCall {
        name: String,
        args: Vec<CompiledExpression>,
    },
    Array(Vec<CompiledExpression>),
    When {
        branches: Vec<WhenBranch>,
        else_expr: Option<Box<CompiledExpression>>,
    },
    IsNull(Box<CompiledExpression>),
    IsNotNull(Box<CompiledExpression>),
    Grouped(Box<CompiledExpression>),
}

/// Binary operators matching AST BinaryOperator
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum BinaryOp {
    // Arithmetic
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    // Comparison
    Equal,
    NotEqual,
    GreaterThan,
    LessThan,
    GreaterOrEqual,
    LessOrEqual,
    // Logical
    And,
    Or,
}

/// Unary operators matching AST UnaryOperator
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum UnaryOp {
    Not,
    Negate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WhenBranch {
    pub condition: CompiledExpression,
    pub value: CompiledExpression,
}
