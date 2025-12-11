use thiserror::Error;

#[derive(Debug, Error)]
pub enum ExpressionError {
    #[error("Missing required environment variable: {0}")]
    MissingRequiredEnvVar(String),

    #[error("Failed to parse environment variable '{var}' with value '{value}' as {expected_type}")]
    EnvParseError {
        var: String,
        value: String,
        expected_type: String,
    },

    #[error("Unknown function: {0}")]
    UnknownFunction(String),

    #[error("Invalid function arguments for {function}: {message}")]
    InvalidFunctionArgs { function: String, message: String },

    #[error("Type mismatch: expected {expected}, got {actual}")]
    TypeMismatch { expected: String, actual: String },

    #[error("Unsupported operation: {0}")]
    UnsupportedOperation(String),

    #[error("Field not found: {0}")]
    FieldNotFound(String),
}

pub type Result<T> = std::result::Result<T, ExpressionError>;
