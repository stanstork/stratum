use thiserror::Error;

#[derive(Debug, Clone)]
pub enum ErrorType {
    Transient, // Retryable (e.g., network issues, timeouts)
    Permanent, // Non-retryable (e.g., data corruption, invalid configuration)
}

#[derive(Error, Debug)]
pub enum TransformError {
    // Transient errors - retry
    #[error("Network error during transformation: {0}")]
    NetworkError(String),

    #[error("Timeout during transformation: {0}")]
    DatabaseTimeout(String),

    #[error("Temporary unavailable resource: {0}")]
    TemporaryUnavailable(String),

    // Permanent errors - send to DLQ immediately
    #[error("Transformation failed: {0}")]
    Transformation(String),

    #[error("Validation failed: {rule} - {message}")]
    ValidationFailed { rule: String, message: String },

    #[error("Row was filtered out")]
    FilteredOut,
}

impl TransformError {
    pub fn error_type(&self) -> ErrorType {
        match self {
            TransformError::NetworkError(_) => ErrorType::Transient,
            TransformError::DatabaseTimeout(_) => ErrorType::Transient,
            TransformError::TemporaryUnavailable(_) => ErrorType::Transient,
            TransformError::Transformation(_) => ErrorType::Permanent,
            TransformError::ValidationFailed { .. } => ErrorType::Permanent,
            TransformError::FilteredOut => ErrorType::Permanent,
        }
    }
}
