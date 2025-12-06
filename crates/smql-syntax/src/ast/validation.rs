use crate::ast::{attribute::Attribute, expr::Expression, span::Span};

#[derive(Debug, Clone, PartialEq)]
pub struct ValidateBlock {
    pub checks: Vec<ValidationCheck>,
    pub span: Span,
}

/// Validation check (assert or warn)
#[derive(Debug, Clone, PartialEq)]
pub struct ValidationCheck {
    pub kind: ValidationKind,
    pub label: String,
    pub body: ValidationBody,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ValidationKind {
    Assert,
    Warn,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ValidationBody {
    pub check: Expression,
    pub message: String,
    pub action: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OnErrorBlock {
    pub retry: Option<RetryBlock>,
    pub failed_rows: Option<FailedRowsBlock>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RetryBlock {
    pub attributes: Vec<Attribute>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FailedRowsBlock {
    pub attributes: Vec<Attribute>,
    pub span: Span,
}
