use thiserror::Error;

/// Top‐level errors for the data migration engine.
#[derive(Debug, Error)]
pub enum ReportGenerationError {
    /// An unknown error occurred.
    #[error("Failed to generate report: {0}")]
    GenerationFailed(String),

    /// Missing required environment variable for report generation.
    #[error("Missing REPORT_CALLBACK_URL environment variable")]
    MissingCallbackUrl,

    /// Missing required authentication token for report generation.
    #[error("Missing AUTH_TOKEN environment variable")]
    MissingAuthToken,
}
