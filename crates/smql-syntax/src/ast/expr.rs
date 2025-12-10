use crate::ast::{
    dotpath::DotPath,
    literal::Literal,
    operator::{BinaryOperator, UnaryOperator},
    span::Span,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Expression {
    pub kind: ExpressionKind,
    pub span: Span,
}

impl Expression {
    pub fn new(kind: ExpressionKind, span: Span) -> Self {
        Expression { kind, span }
    }
}

/// Expression types
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ExpressionKind {
    Literal(Literal),
    Identifier(String),
    DotNotation(DotPath),
    Binary {
        left: Box<Expression>,
        operator: BinaryOperator,
        right: Box<Expression>,
    },
    Unary {
        operator: UnaryOperator,
        operand: Box<Expression>,
    },
    FunctionCall {
        name: String,
        arguments: Vec<Expression>,
    },
    Array(Vec<Expression>),
    WhenExpression {
        branches: Vec<WhenBranch>,
        else_value: Option<Box<Expression>>,
    },
    IsNull(Box<Expression>),
    IsNotNull(Box<Expression>),
    Grouped(Box<Expression>),
}

/// When expression branch
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WhenBranch {
    pub condition: Expression,
    pub value: Expression,
    pub span: Span,
}
