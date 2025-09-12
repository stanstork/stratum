use chrono::{DateTime, Utc};
use common::{row_data::RowData, value::Value};
use serde::Serialize;
use std::collections::HashMap;

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
pub enum Severity {
    Info,
    Warning,
    Error,
}

#[derive(Serialize, Debug, Clone, PartialEq, Eq)]
pub enum FindingKind {
    SourceSchema, // e.g., nullable mismatch, missing PK
    DestinationSchema,
    SourceData,     // e.g., missing data, type mismatch
    Mapping,        // field map issues
    Transformation, // pipeline issues
    Connectivity,   // auth/connection
    Other,
}

#[derive(Serialize, Debug, Clone, PartialEq, Eq)]
pub struct Location {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entity: Option<String>, // table / collection
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>, // column / property
}

#[derive(Serialize, Debug, Clone)]
pub struct Finding {
    pub code: String,    // stable programmatic id
    pub message: String, // human-readable
    pub severity: Severity,
    pub kind: FindingKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<Location>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suggestion: Option<String>, // how to fix
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
pub struct EntityMappingReport {
    pub source_entity: String,
    pub dest_entity: String,

    pub copy_policy: String, // "ALL" | "MAP_ONLY"

    pub mapped_fields: usize,
    pub created_fields: usize, // from computed
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub renames: Vec<FieldRename>,

    // When CopyColumns=MapOnly
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub omitted_source_columns: Vec<String>, // ignored in dest

    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub computed: Vec<ComputedPreview>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub coercions: Vec<CoercionHint>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

#[derive(Serialize, Debug, Clone)]
pub struct FieldRename {
    pub from: String,
    pub to: String,
}

#[derive(Serialize, Debug, Clone)]
pub struct ComputedPreview {
    pub name: String,
    pub expression_preview: String, // pretty-printed/shortened
}

#[derive(Serialize, Debug, Clone)]
pub struct CoercionHint {
    pub field: String,
    pub from_type: String,
    pub to_type: String,
}

#[derive(Serialize, Debug, Clone)]
pub struct LookupMappingReport {
    pub source_entity: String,
    pub entity: String,
    pub key: String,
    pub target: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

#[derive(Serialize, Debug, Default, Clone)]
pub struct MappingReport {
    pub totals: MappingTotals,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub entities: Vec<EntityMappingReport>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub lookups: Vec<LookupMappingReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mapping_hash: Option<String>,
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
