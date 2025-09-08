use common::{
    row_data::RowData,
    value::{FieldValue, Value},
};
use serde::Serialize;
use sqlx::Row;
use std::collections::HashMap;

/// The overall status of the validation run.
#[derive(Serialize, Debug, Clone, PartialEq)]
pub enum ValidationStatus {
    Success,
    SuccessWithWarnings,
    Failure,
}

impl Default for ValidationStatus {
    fn default() -> Self {
        ValidationStatus::Success
    }
}

#[derive(Serialize, Debug, Clone)]
pub struct SchemaAction {
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entity: Option<String>,
}

#[derive(Serialize, Debug, Default, Clone)]
pub struct ValidationSummary {
    pub status: ValidationStatus,
    pub timestamp: String,
    pub source_type: String,
    pub destination_type: String,
    pub records_sampled: usize,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<String>,
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
    /// Queries related to schema definition (CREATE/ALTER TABLE, etc.).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub schema_queries: Vec<(String, Option<Vec<Value>>)>,
    /// Queries related to data manipulation (INSERT, UPDATE, DELETE).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub data_queries: Vec<(String, Option<Vec<Value>>)>,
}

#[derive(Serialize, Debug, Clone)]
pub struct TransformationRecord {
    pub input_record: RowData,
    pub output_record: Option<RowData>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Serialize, Debug, Clone, Default)]
pub struct TransformationSummary {
    pub successful_transformations: usize,
    pub failed_transformations: usize,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub transformed_sample_data: Vec<TransformationRecord>,
}

#[derive(Serialize, Debug, Clone, Default)]
pub struct ValidationReport {
    pub summary: ValidationSummary,
    pub schema_analysis: SchemaAnalysis,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub transformed_sample_data: Vec<HashMap<String, RowData>>,
    pub generated_queries: GeneratedQueries,
    pub transformation_summary: TransformationSummary,
}
