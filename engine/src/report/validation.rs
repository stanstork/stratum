use common::value::Value;
use serde::Serialize;
use std::collections::HashMap;

/// The overall status of the validation run.
#[derive(Serialize, Debug, Clone, PartialEq)]
pub enum ValidationStatus {
    Success,
    SuccessWithWarnings,
    Failure,
}

#[derive(Serialize, Debug, Clone)]
pub struct SchemaAction {
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub column: Option<String>,
}

#[derive(Serialize, Debug, Clone)]
pub struct ValidationSummary {
    pub status: ValidationStatus,
    pub timestamp: String,
    pub source_type: String,
    pub destination_type: String,
    pub records_sampled: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Serialize, Debug, Clone, Default)]
pub struct SchemaAnalysis {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub source_warnings: Vec<SchemaAction>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub destination_warnings: Vec<SchemaAction>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<SchemaAction>,
}

#[derive(Serialize, Debug, Clone, Default)]
pub struct GeneratedQueries {
    pub ddl: Vec<(String, Option<Vec<Value>>)>,
}

#[derive(Serialize, Debug, Clone, Default)]
pub struct ValidationReport {
    pub validation_summary: Option<ValidationSummary>,
    pub schema_analysis: SchemaAnalysis,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub transformed_sample_data: Vec<HashMap<String, serde_json::Value>>,
    pub generated_queries: GeneratedQueries,
}
