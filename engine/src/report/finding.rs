use serde::Serialize;

#[derive(Serialize, Debug, Clone, PartialEq, Eq, Hash)]
pub enum Severity {
    Info,
    Warning,
    Error,
}

#[derive(Serialize, Debug, Clone, PartialEq, Eq, Hash)]
pub enum FindingKind {
    SourceSchema, // e.g., nullable mismatch, missing PK
    DestinationSchema,
    SourceData,     // e.g., missing data, type mismatch
    Mapping,        // field map issues
    Transformation, // pipeline issues
    Connectivity,   // auth/connection
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

impl Finding {
    pub fn error(code: &str, message: &str) -> Self {
        Finding {
            code: code.to_string(),
            message: message.to_string(),
            severity: Severity::Error,
            kind: FindingKind::DestinationSchema,
            suggestion: None,
        }
    }

    pub fn warning(code: &str, message: &str) -> Self {
        Finding {
            code: code.to_string(),
            message: message.to_string(),
            severity: Severity::Warning,
            kind: FindingKind::DestinationSchema,
            suggestion: None,
        }
    }
}

pub struct MappingFinding;
pub struct FetchFinding;
pub struct SourceSchemaFinding;

const CODE_MAPPING_MISSING: &str = "MAPPING_MISSING";
const CODE_FETCH_ERROR: &str = "FETCH_ERROR";
const CODE_UNSUPPORTED_SOURCE: &str = "UNSUPPORTED_SOURCE";

impl MappingFinding {
    pub fn create_missing_finding(table: &str, extra_message: &str) -> Finding {
        Finding {
            code: CODE_MAPPING_MISSING.into(),
            message: format!(
                "No mapping found for table `{table}` while `mapped_columns_only` is set.{extra_message}"
            ),
            severity: Severity::Error,
            kind: FindingKind::SourceSchema,
            suggestion: Some("Add field mappings for this table or disable `mapped_columns_only`.".into()),
        }
    }
}

impl FetchFinding {
    pub fn create_error_finding(error_message: &str) -> Finding {
        Finding {
            code: CODE_FETCH_ERROR.into(),
            message: format!("Error fetching data: {error_message}"),
            severity: Severity::Error,
            kind: FindingKind::SourceData,
            suggestion: Some("Check source connectivity and query validity.".into()),
        }
    }
}

impl SourceSchemaFinding {
    pub fn create_unsupported_finding(source_format: String) -> Finding {
        Finding {
            code: CODE_UNSUPPORTED_SOURCE.into(),
            message: format!(
                "Validation run does not support source type: {:?}",
                source_format
            ),
            severity: Severity::Error,
            kind: FindingKind::SourceSchema,
            suggestion: Some("Use a database source for validation runs.".into()),
        }
    }
}
