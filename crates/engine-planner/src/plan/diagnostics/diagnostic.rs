use crate::plan::diagnostics::{level::DiagnosticLevel, location::SourceLocation};
use serde::Serialize;

#[derive(Serialize, Debug, Clone)]
pub struct Diagnostic {
    pub level: DiagnosticLevel,
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suggestion: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pipeline: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<SourceLocation>,
}

impl Diagnostic {
    pub fn error(code: &str, message: &str) -> Self {
        Self {
            level: DiagnosticLevel::Error,
            code: code.to_string(),
            message: message.to_string(),
            suggestion: None,
            pipeline: None,
            location: None,
        }
    }

    pub fn warning(code: &str, message: &str) -> Self {
        Self {
            level: DiagnosticLevel::Warning,
            code: code.to_string(),
            message: message.to_string(),
            suggestion: None,
            pipeline: None,
            location: None,
        }
    }

    pub fn info(code: &str, message: &str) -> Self {
        Self {
            level: DiagnosticLevel::Info,
            code: code.to_string(),
            message: message.to_string(),
            suggestion: None,
            pipeline: None,
            location: None,
        }
    }

    pub fn hint(code: &str, message: &str) -> Self {
        Self {
            level: DiagnosticLevel::Hint,
            code: code.to_string(),
            message: message.to_string(),
            suggestion: None,
            pipeline: None,
            location: None,
        }
    }

    pub fn with_suggestion(mut self, suggestion: &str) -> Self {
        self.suggestion = Some(suggestion.to_string());
        self
    }

    pub fn with_pipeline(mut self, pipeline: &str) -> Self {
        self.pipeline = Some(pipeline.to_string());
        self
    }
}
