use crate::ast::span::Span;
use std::fmt;
use thiserror::Error;

/// Severity level for validation issues
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    Error,
    Warning,
}

impl fmt::Display for Severity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Severity::Error => write!(f, "error"),
            Severity::Warning => write!(f, "warning"),
        }
    }
}

/// Type of semantic validation issue
#[derive(Debug, Clone, PartialEq)]
pub enum ValidationIssueKind {
    // Required field validation
    MissingRequiredField { block_type: String, field: String },

    // Duplicate names
    DuplicateConnection { name: String, first_location: Span },
    DuplicatePipeline { name: String, first_location: Span },
    DuplicateDefineAttribute { name: String, first_location: Span },

    // Reference validation
    UndefinedConnection { name: String },
    UndefinedPipeline { name: String },
    UndefinedDefineConstant { name: String },
    UndefinedVariable { name: String },

    // Circular dependency
    CircularDefineDependency { chain: Vec<String> },
    CircularPipelineDependency { chain: Vec<String> },

    // Type mismatches
    InvalidExpressionInContext { expected: String, found: String },

    // Best practices (warnings)
    UnusedConnection { name: String },
    UnusedDefineConstant { name: String },
    EmptyBlock { block_type: String },
}

impl fmt::Display for ValidationIssueKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ValidationIssueKind::MissingRequiredField { block_type, field } => {
                write!(
                    f,
                    "missing required field '{}' in {} block",
                    field, block_type
                )
            }
            ValidationIssueKind::DuplicateConnection { name, .. } => {
                write!(f, "duplicate connection '{}'", name)
            }
            ValidationIssueKind::DuplicatePipeline { name, .. } => {
                write!(f, "duplicate pipeline '{}'", name)
            }
            ValidationIssueKind::DuplicateDefineAttribute { name, .. } => {
                write!(f, "duplicate define attribute '{}'", name)
            }
            ValidationIssueKind::UndefinedConnection { name } => {
                write!(f, "undefined connection '{}'", name)
            }
            ValidationIssueKind::UndefinedPipeline { name } => {
                write!(f, "undefined pipeline '{}'", name)
            }
            ValidationIssueKind::UndefinedDefineConstant { name } => {
                write!(f, "undefined define constant '{}'", name)
            }
            ValidationIssueKind::UndefinedVariable { name } => {
                write!(f, "undefined variable '{}'", name)
            }
            ValidationIssueKind::CircularDefineDependency { chain } => {
                write!(f, "circular dependency in define: {}", chain.join(" -> "))
            }
            ValidationIssueKind::CircularPipelineDependency { chain } => {
                write!(f, "circular dependency in pipeline: {}", chain.join(" -> "))
            }
            ValidationIssueKind::InvalidExpressionInContext { expected, found } => {
                write!(f, "expected {} but found {}", expected, found)
            }
            ValidationIssueKind::UnusedConnection { name } => {
                write!(f, "unused connection '{}'", name)
            }
            ValidationIssueKind::UnusedDefineConstant { name } => {
                write!(f, "unused define constant '{}'", name)
            }
            ValidationIssueKind::EmptyBlock { block_type } => {
                write!(f, "empty {} block", block_type)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ValidationIssue {
    pub severity: Severity,
    pub kind: ValidationIssueKind,
    pub span: Span,
    pub message: String,
}

impl ValidationIssue {
    pub fn error(kind: ValidationIssueKind, span: Span) -> Self {
        let message = kind.to_string();
        Self {
            severity: Severity::Error,
            kind,
            span,
            message,
        }
    }

    pub fn warning(kind: ValidationIssueKind, span: Span) -> Self {
        let message = kind.to_string();
        Self {
            severity: Severity::Warning,
            kind,
            span,
            message,
        }
    }

    pub fn with_message(mut self, message: String) -> Self {
        self.message = message;
        self
    }
}

impl fmt::Display for ValidationIssue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} at line {}, column {}: {}",
            self.severity, self.span.line, self.span.column, self.message
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ValidationResult {
    pub errors: Vec<ValidationIssue>,
    pub warnings: Vec<ValidationIssue>,
}

impl ValidationResult {
    pub fn new() -> Self {
        Self {
            errors: Vec::new(),
            warnings: Vec::new(),
        }
    }

    pub fn add_error(&mut self, issue: ValidationIssue) {
        self.errors.push(issue);
    }

    pub fn add_warning(&mut self, issue: ValidationIssue) {
        self.warnings.push(issue);
    }

    pub fn add_issue(&mut self, issue: ValidationIssue) {
        match issue.severity {
            Severity::Error => self.errors.push(issue),
            Severity::Warning => self.warnings.push(issue),
        }
    }

    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }

    pub fn is_valid(&self) -> bool {
        !self.has_errors()
    }

    pub fn merge(&mut self, other: ValidationResult) {
        self.errors.extend(other.errors);
        self.warnings.extend(other.warnings);
    }
}

impl Default for ValidationResult {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for ValidationResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.errors.is_empty() && self.warnings.is_empty() {
            return write!(f, "No validation issues");
        }

        for error in &self.errors {
            writeln!(f, "{}", error)?;
        }

        for warning in &self.warnings {
            writeln!(f, "{}", warning)?;
        }

        Ok(())
    }
}

/// Error type for AST building
#[derive(Debug, Clone)]
pub struct BuildError {
    pub message: String,
    pub line: usize,
    pub column: usize,
}

impl std::fmt::Display for BuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Parse error at line {}, column {}: {}",
            self.line, self.column, self.message
        )
    }
}

impl std::error::Error for BuildError {}

#[derive(Error, Debug)]
pub enum SmqlError {
    #[error("Parsing error: {0}")]
    Parse(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_severity_display() {
        assert_eq!(format!("{}", Severity::Error), "error");
        assert_eq!(format!("{}", Severity::Warning), "warning");
    }

    #[test]
    fn test_validation_issue_creation() {
        let span = Span::new(0, 10, 1, 1);
        let issue = ValidationIssue::error(
            ValidationIssueKind::UndefinedConnection {
                name: "test".to_string(),
            },
            span,
        );

        assert_eq!(issue.severity, Severity::Error);
        assert!(issue.message.contains("undefined connection"));
    }

    #[test]
    fn test_validation_result() {
        let mut result = ValidationResult::new();
        assert!(result.is_valid());
        assert!(!result.has_errors());

        let span = Span::new(0, 10, 1, 1);
        result.add_error(ValidationIssue::error(
            ValidationIssueKind::UndefinedConnection {
                name: "test".to_string(),
            },
            span,
        ));

        assert!(!result.is_valid());
        assert!(result.has_errors());
        assert_eq!(result.errors.len(), 1);
    }

    #[test]
    fn test_validation_result_merge() {
        let mut result1 = ValidationResult::new();
        let mut result2 = ValidationResult::new();

        let span = Span::new(0, 10, 1, 1);
        result1.add_error(ValidationIssue::error(
            ValidationIssueKind::UndefinedConnection {
                name: "test1".to_string(),
            },
            span,
        ));

        result2.add_warning(ValidationIssue::warning(
            ValidationIssueKind::UnusedConnection {
                name: "test2".to_string(),
            },
            span,
        ));

        result1.merge(result2);

        assert_eq!(result1.errors.len(), 1);
        assert_eq!(result1.warnings.len(), 1);
    }
}
