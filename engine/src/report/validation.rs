use chrono::{DateTime, Utc};
use common::{row_data::RowData, value::Value};
use serde::Serialize;
use std::collections::HashMap;

use crate::report::{finding::Finding, mapping::MappingReport};

/// The overall status of the validation run.
#[derive(Serialize, Debug, Clone, PartialEq)]
pub enum DryRunStatus {
    Success,
    SuccessWithWarnings,
    Failure,
}

impl Default for DryRunStatus {
    fn default() -> Self {
        DryRunStatus::Success
    }
}

#[derive(Serialize, Debug, Clone, PartialEq, Eq)]
pub enum EndpointType {
    Database { dialect: String }, // "Postgres", "MySQL", etc.
    Api { name: String },
    File { format: String }, // "CSV", "Parquet"
    Other(String),
}

impl Default for EndpointType {
    fn default() -> Self {
        EndpointType::Other("Unknown".to_string())
    }
}

#[derive(Serialize, Debug, Default, Clone)]
pub struct DryRunSummary {
    pub status: DryRunStatus,
    pub timestamp: DateTime<Utc>,
    pub source: EndpointType,
    pub destination: EndpointType,
    pub records_sampled: usize,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<Finding>,
    pub rows_estimated: Option<u64>, // estimated total rows to migrate
    pub batch_count: Option<u64>,    // number of batches planned
    pub expected_writes: Option<u64>, // estimated write operations
}

#[derive(Serialize, Debug, Clone)]
pub struct SchemaAction {
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entity: Option<String>,
}

#[derive(Serialize, Debug, Default, Clone)]
pub struct SchemaReview {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub source_findings: Vec<Finding>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub destination_findings: Vec<Finding>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<SchemaAction>, // actionable changes (e.g., "ADD COLUMN")
}

#[derive(Serialize, Debug, Clone, PartialEq, Eq)]
pub enum SqlKind {
    Schema,
    Data,
}

impl Default for SqlKind {
    fn default() -> Self {
        SqlKind::Data
    }
}

#[derive(Serialize, Debug, Clone)]
pub struct SqlStatement {
    pub dialect: String, // "MySQL", "Postgres", ...
    pub kind: SqlKind,   // Schema | Data
    pub sql: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub params: Vec<Value>, // normalized; empty if none
}

#[derive(Serialize, Debug, Default, Clone)]
pub struct GeneratedSql {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub statements: Vec<SqlStatement>,
}

#[derive(Serialize, Debug, Clone)]
pub struct TransformationRecord {
    pub input: RowData,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<RowData>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warnings: Option<Vec<String>>,
}

#[derive(Serialize, Debug, Default, Clone)]
pub struct TransformationReport {
    pub ok: usize,
    pub failed: usize,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub sample: Vec<TransformationRecord>,
}

#[derive(Serialize, Debug, Default, Clone)]
pub struct DataProfile {
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub entities: HashMap<String, usize>, // table -> row count (sampled)
}

#[derive(Serialize, Debug, Default, Clone)]
pub struct MappingTotals {
    pub entities: usize,
    pub mapped_fields: usize,
    pub computed_fields: usize,
    pub lookup_count: usize,
}

#[derive(Serialize, Debug, Default, Clone)]
pub struct DryRunReport {
    pub run_id: String,
    pub engine_version: String,
    pub config_hash: String, // hash of the smql config

    // content
    pub summary: DryRunSummary,
    pub mapping: MappingReport,
    pub schema: SchemaReview,
    pub generated_sql: GeneratedSql,
    pub transform: TransformationReport,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile: Option<DataProfile>,
}
