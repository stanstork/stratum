use serde::Serialize;

#[derive(Serialize, Debug, Clone, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum Severity {
    Info,
    Warning,
    Error,
}

#[derive(Serialize, Debug, Clone, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum FindingKind {
    SourceSchema, // e.g., nullable mismatch, missing PK
    DestinationSchema,
    SourceData,     // e.g., missing data, type mismatch
    Mapping,        // field map issues
    Transformation, // pipeline issues
    Connectivity,   // auth/connection
    SampleData,     // issues found during dry run sampling
    Other,
}

#[derive(Serialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct Finding {
    pub code: String,    // stable programmatic id
    pub message: String, // human-readable
    pub severity: Severity,
    pub kind: FindingKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suggestion: Option<String>, // how to fix
}

/// Constants for finding codes.
const CODE_MAPPING_MISSING: &str = "MAPPING_MISSING";
const CODE_FETCH_ERROR: &str = "FETCH_ERROR";
const CODE_UNSUPPORTED_SOURCE: &str = "UNSUPPORTED_SOURCE";

impl Finding {
    pub fn new(
        code: &str,
        message: String,
        severity: Severity,
        kind: FindingKind,
        suggestion: Option<String>,
    ) -> Self {
        Finding {
            code: code.to_string(),
            message,
            severity,
            kind,
            suggestion,
        }
    }

    /// Creates a new finding for a missing table mapping.
    pub fn new_mapping_missing(table: &str, extra_message: &str) -> Self {
        Self::new(
            CODE_MAPPING_MISSING,
            format!(
                "No mapping found for table `{table}` while `mapped_columns_only` is set.{extra_message}"
            ),
            Severity::Error,
            FindingKind::Mapping,
            Some("Add field mappings for this table or disable `mapped_columns_only`.".into()),
        )
    }

    /// Creates a new finding for a data fetching error.
    pub fn new_fetch_error(error_message: &str) -> Self {
        Self::new(
            CODE_FETCH_ERROR,
            format!("Error fetching data: {error_message}"),
            Severity::Error,
            FindingKind::SourceData,
            Some("Check source connectivity and query validity.".into()),
        )
    }

    /// Creates a new finding for an unsupported source type.
    pub fn new_unsupported_source(source_format: &str) -> Self {
        Self::new(
            CODE_UNSUPPORTED_SOURCE,
            format!("Validation run does not support source type: {source_format}"),
            Severity::Error,
            FindingKind::SourceSchema,
            Some("Use a database source for validation runs.".into()),
        )
    }

    /// Creates a standardized error finding.
    pub fn error(code: &str, message: &str, kind: FindingKind) -> Self {
        Finding::new(code, message.to_string(), Severity::Error, kind, None)
    }

    /// Creates a standardized warning finding.
    pub fn warning(code: &str, message: &str, kind: FindingKind) -> Self {
        Finding::new(code, message.to_string(), Severity::Warning, kind, None)
    }
}
