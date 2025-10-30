use crate::report::{
    finding::Finding,
    mapping::MappingReport,
    schema::{SchemaReview, SchemaValidationReport},
    sql::GeneratedSql,
    transform::TransformationReport,
};
use chrono::{DateTime, Utc};
use engine_core::connectors::{
    destination::{DataDestination, Destination},
    source::{DataSource, Source},
};
use model::transform::mapping::EntityMapping;
use serde::Serialize;
use smql_syntax::ast::setting::CopyColumns;

/// The overall status of the validation run.
#[derive(Serialize, Debug, Clone, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub enum DryRunStatus {
    #[default]
    Success,
    SuccessWithWarnings,
    Failure,
}

/// Describes the type of endpoint used in the dry run.
#[derive(Serialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
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

/// A summary of the dry run execution.
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

/// The main report structure for a dry run.
#[derive(Serialize, Debug, Default, Clone)]
pub struct DryRunReport {
    pub run_id: String,
    pub engine_version: String,
    pub config_hash: String, // hash of the smql config
    pub summary: DryRunSummary,
    pub mapping: MappingReport,
    pub schema: SchemaReview,
    pub generated_sql: GeneratedSql,
    pub transform: TransformationReport,
    pub schema_validation: SchemaValidationReport,
}

/// Parameters required to generate a dry run report.
/// This struct groups the necessary data for creating a `DryRunReport`.
pub struct DryRunParams<'a> {
    pub source: EndpointType,
    pub destination: EndpointType,
    pub mapping: &'a EntityMapping,
    pub config_hash: &'a str,
    pub copy_columns: CopyColumns,
}

impl DryRunReport {
    pub fn new(params: DryRunParams) -> Self {
        let summary = DryRunSummary {
            timestamp: Utc::now(),
            ..Default::default()
        };

        DryRunReport {
            run_id: uuid::Uuid::new_v4().to_string(),
            config_hash: params.config_hash.to_string(),
            engine_version: env!("CARGO_PKG_VERSION").to_string(),
            summary,
            mapping: MappingReport::from_mapping(params.mapping, &params.copy_columns),
            ..Default::default()
        }
    }
}

pub fn source_endpoint(source: &Source) -> EndpointType {
    match &source.primary {
        DataSource::Database(_) => EndpointType::Database {
            dialect: source.dialect().name(),
        },
        DataSource::File(_) => EndpointType::File {
            format: "CSV".to_string(), // Currently CSV is the only supported file type
        },
    }
}

pub fn dest_endpoint(destination: &Destination) -> EndpointType {
    match &destination.data_dest {
        DataDestination::Database(_) => EndpointType::Database {
            dialect: destination.dialect().name(),
        },
    }
}
