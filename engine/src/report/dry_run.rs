use crate::{
    destination::{data::DataDestination, Destination},
    error::MigrationError,
    report::{
        finding::Finding, mapping::MappingReport, schema::SchemaReview, sql::GeneratedSql,
        transform::TransformationReport,
    },
    source::{data::DataSource, Source},
};
use chrono::{DateTime, Utc};
use common::mapping::EntityMapping;
use serde::Serialize;
use smql::statements::setting::CopyColumns;

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
}

/// A container for the parameters required to generate a dry run report.
pub struct DryRunParams<'a> {
    pub source: &'a Source,
    pub destination: &'a Destination,
    pub mapping: &'a EntityMapping,
    pub config_hash: &'a str,
}

impl DryRunReport {
    pub fn new(params: DryRunParams, copy_columns: &CopyColumns) -> Result<Self, MigrationError> {
        let source_endpoint = match &params.source.primary {
            DataSource::Database(_) => EndpointType::Database {
                dialect: params.source.dialect().name(),
            },
            DataSource::File(_) => EndpointType::File {
                format: "Unknown".to_string(), // TODO: extract from file type
            },
        };

        let dest_endpoint = match &params.destination.data_dest {
            DataDestination::Database(_) => EndpointType::Database {
                dialect: params.destination.dialect().name(),
            },
        };

        let report = DryRunReport {
            run_id: uuid::Uuid::new_v4().to_string(),
            config_hash: params.config_hash.to_string(),
            engine_version: env!("CARGO_PKG_VERSION").to_string(),
            summary: DryRunSummary {
                source: source_endpoint,
                destination: dest_endpoint,
                timestamp: chrono::Utc::now(),
                ..Default::default()
            },
            mapping: MappingReport::from_mapping(params.mapping, copy_columns),
            ..Default::default()
        };
        Ok(report)
    }
}
