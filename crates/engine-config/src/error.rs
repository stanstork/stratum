use thiserror::Error;

/// Top‐level errors for the data migration engine.
#[derive(Debug, Error)]
pub enum ReportGenerationError {
    #[error("Failed to generate report: {0}")]
    GenerationFailed(String),

    #[error("Missing REPORT_CALLBACK_URL environment variable")]
    MissingCallbackUrl,

    #[error("Missing AUTH_TOKEN environment variable")]
    MissingAuthToken,
}
