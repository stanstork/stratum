use async_trait::async_trait;
use std::fmt;

/// Unified trait for all pipeline analyzers
///
/// This trait provides a common interface for all analyzer components,
/// enabling registry-based orchestration and composition.
#[async_trait]
pub trait PlanAnalyzer: Send + Sync {
    /// Input type for this analyzer
    type Input;

    /// Output type (the plan this analyzer produces)
    type Output;

    /// Human-readable name of this analyzer for logging/debugging
    fn name(&self) -> &'static str;

    /// Analyze the input and produce a plan
    ///
    /// # Arguments
    /// * `input` - The specific input this analyzer needs (e.g., DataSource, Pipeline, etc.)
    /// * `ctx` - Shared analysis context with adapters, caches, and configuration
    ///
    /// # Returns
    /// The analyzer's output plan or an error if analysis fails
    async fn analyze(
        &self,
        input: &Self::Input,
        ctx: &super::context::AnalysisContext,
    ) -> AnalyzerResult<Self::Output>;
}

/// Result type for analyzer operations
pub type AnalyzerResult<T> = Result<T, AnalyzerError>;

/// Error that can occur during analysis
#[derive(Debug, Clone)]
pub struct AnalyzerError {
    /// Name of the analyzer that produced the error
    pub analyzer: String,

    /// Error message
    pub message: String,

    /// Severity of the error
    pub severity: Severity,

    /// Optional location in source config where error occurred
    pub location: Option<SourceLocation>,
}

impl AnalyzerError {
    /// Create a new analyzer error
    pub fn new(analyzer: &str, message: String, severity: Severity) -> Self {
        Self {
            analyzer: analyzer.to_string(),
            message,
            severity,
            location: None,
        }
    }

    /// Create an error-level analyzer error
    pub fn error(analyzer: &str, message: String) -> Self {
        Self::new(analyzer, message, Severity::Error)
    }

    /// Create a warning-level analyzer error
    pub fn warning(analyzer: &str, message: String) -> Self {
        Self::new(analyzer, message, Severity::Warning)
    }

    /// Add location information to this error
    pub fn with_location(mut self, location: SourceLocation) -> Self {
        self.location = Some(location);
        self
    }
}

impl fmt::Display for AnalyzerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}: {}", self.analyzer, self.severity, self.message)
    }
}

impl std::error::Error for AnalyzerError {}

/// Severity level for errors and diagnostics
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Severity {
    /// Informational message
    Info,
    /// Warning that doesn't prevent execution
    Warning,
    /// Error that prevents execution
    Error,
}

impl fmt::Display for Severity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Severity::Info => write!(f, "INFO"),
            Severity::Warning => write!(f, "WARNING"),
            Severity::Error => write!(f, "ERROR"),
        }
    }
}

/// Location in source configuration file
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceLocation {
    /// File path
    pub file: String,
    /// Line number (1-indexed)
    pub line: usize,
    /// Column number (1-indexed)
    pub column: usize,
}

impl SourceLocation {
    pub fn new(file: String, line: usize, column: usize) -> Self {
        Self { file, line, column }
    }
}

impl fmt::Display for SourceLocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}:{}", self.file, self.line, self.column)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_severity_ordering() {
        assert!(Severity::Info < Severity::Warning);
        assert!(Severity::Warning < Severity::Error);
    }

    #[test]
    fn test_error_display() {
        let err = AnalyzerError::error("test_analyzer", "something went wrong".to_string());
        let display = format!("{}", err);
        assert!(display.contains("test_analyzer"));
        assert!(display.contains("ERROR"));
        assert!(display.contains("something went wrong"));
    }

    #[test]
    fn test_location_display() {
        let loc = SourceLocation::new("pipeline.smql".to_string(), 42, 10);
        assert_eq!(format!("{}", loc), "pipeline.smql:42:10");
    }
}
