use model::execution::expr::CompiledExpression;
use thiserror::Error;

pub mod csv;
pub mod sql;

/// Error raised when a filter expression cannot be compiled into a backend
/// filter (e.g. an unsupported function or expression in a `where` clause).
#[derive(Debug, Error)]
pub enum FilterCompileError {
    #[error("unsupported function call in filter: {0}")]
    UnsupportedFunction(String),

    #[error("unsupported expression in filter: {0}")]
    UnsupportedExpression(String),

    #[error("unsupported operator in filter: {0}")]
    UnsupportedOperator(String),

    #[error("unsupported value in filter: {0}")]
    UnsupportedValue(String),

    #[error("invalid filter field: {0}")]
    InvalidField(String),
}

/// A trait for compiling filter expressions into a specific format.
pub trait FilterCompiler {
    /// The type of filter that this compiler produces.
    type Filter;

    /// Compile the AST into a filter, returning an error for unsupported input.
    fn compile(expr: &CompiledExpression) -> Result<Self::Filter, FilterCompileError>;
}
