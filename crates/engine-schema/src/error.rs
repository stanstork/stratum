use thiserror::Error;

#[derive(Debug, Error)]
pub enum DependencyError {
    #[error("Circular dependency detected: {0}")]
    CircularDependency(String),

    #[error("Missing dependency: {table} depends on {dependency} which doesn't exist")]
    MissingDependency { table: String, dependency: String },
}

#[derive(Error, Debug)]
pub enum TypeConversionError {
    #[error("Unsupported type conversion: {from_type} -> {to_type}")]
    Unsupported { from_type: String, to_type: String },

    #[error("Lossy conversion: {from_type} -> {to_type}, reason: {reason}")]
    LossyConversion {
        from_type: String,
        to_type: String,
        reason: String,
    },

    #[error("Type not found: {0}")]
    NotFound(String),
}
