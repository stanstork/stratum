use crate::ast::{expr::Expression, ident::Identifier, span::Span};

/// Attribute assignment (key = value)
#[derive(Debug, Clone, PartialEq)]
pub struct Attribute {
    pub key: Identifier,
    pub value: Expression,
    pub span: Span,
}
