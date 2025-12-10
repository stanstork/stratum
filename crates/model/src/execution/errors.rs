use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConvertError {
    #[error("failed to convert AST to execution plan: {0}")]
    Plan(String),

    #[error("expression evaluation error: {0}")]
    Expression(String),

    #[error("connection error: {0}")]
    Connection(String),
}
