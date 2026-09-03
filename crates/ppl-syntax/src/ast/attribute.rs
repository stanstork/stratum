use crate::ast::{expr::Expression, ident::Identifier, span::Span};
use serde::{Deserialize, Serialize};

/// Attribute assignment (key = value)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Attribute {
    pub key: Identifier,
    pub value: Expression,
    pub span: Span,
}
