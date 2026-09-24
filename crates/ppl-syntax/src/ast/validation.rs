use crate::ast::{attribute::Attribute, expr::Expression, pipeline::NestedBlock, span::Span};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ValidateBlock {
    pub checks: Vec<ValidationCheck>,
    pub span: Span,
}

/// Validation check (assert or warn)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ValidationCheck {
    pub kind: ValidationKind,
    pub label: String,
    pub body: ValidationBody,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ValidationKind {
    Assert,
    Warn,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ValidationBody {
    pub check: Expression,
    pub message: String,
    pub action: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OnErrorBlock {
    pub retry: Option<RetryBlock>,
    pub failed_rows: Option<FailedRowsBlock>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RetryBlock {
    pub attributes: Vec<Attribute>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FailedRowsBlock {
    pub attributes: Vec<Attribute>,
    pub nested_blocks: Vec<NestedBlock>,
    pub span: Span,
}
