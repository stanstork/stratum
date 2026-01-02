use crate::plan::sample::issue::SampleIssue;
use serde::Serialize;
use std::collections::HashMap;

#[derive(Serialize, Debug, Clone)]
pub struct SampleRow {
    pub index: usize,

    /// Primary key or unique identifier from source
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_id: Option<String>,

    /// Source row data (before transformations)
    pub input: HashMap<String, SampleValue>,

    /// Transformed row data (what will be written to destination)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<HashMap<String, SampleValue>>,

    pub status: SampleRowStatus,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub validations: Vec<SampleValidationResult>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub issues: Vec<SampleIssue>,
}

#[derive(Serialize, Debug, Clone)]
pub struct SampleValue {
    pub display: String,
    pub value_type: String,
    pub is_null: bool,
    /// Whether value was truncated for display
    pub truncated: bool,
    /// Original length before truncation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub original_length: Option<usize>,
}

#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SampleRowStatus {
    Ok,
    Warning,
    Skipped,
    Failed,
}

#[derive(Serialize, Debug, Clone)]
pub struct SampleValidationResult {
    pub name: String,
    pub passed: bool,
    pub check: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub actual_values: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}
