use thiserror::Error;

#[derive(Debug, Error)]
pub enum FileError {
    #[error("File not found: {0}")]
    NotFound(String),
    #[error("Permission denied: {0}")]
    PermissionDenied(String),
    #[error("Invalid file format: {0}")]
    InvalidFormat(String),
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Lock error: {0}")]
    LockError(String),
    #[error("CSV parsing error: {0}")]
    CsvError(#[from] csv::Error),
    #[error("Error reading CSV file: {0}")]
    ReadError(String),
    #[error("Invalid cursor for file source: {0}")]
    InvalidCursor(String),
}
