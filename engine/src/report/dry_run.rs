use crate::{
    destination::{data::DataDestination, Destination},
    error::ReportGenerationError,
    report::{
        finding::Finding,
        mapping::MappingReport,
        schema::{SchemaReview, SchemaValidationReport},
        sql::GeneratedSql,
        transform::TransformationReport,
    },
    source::{data::DataSource, Source},
};
use chrono::{DateTime, Utc};
use common::mapping::EntityMapping;
use serde::Serialize;
use smql::statements::setting::CopyColumns;

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
    pub source: &'a Source,
    pub destination: &'a Destination,
    pub mapping: &'a EntityMapping,
    pub config_hash: &'a str,
}

impl DryRunReport {
    pub fn new(
        params: DryRunParams,
        copy_columns: &CopyColumns,
    ) -> Result<Self, ReportGenerationError> {
        let source_endpoint = Self::source_endpoint(params.source)?;
        let dest_endpoint = Self::dest_endpoint(params.destination)?;

        let summary = DryRunSummary {
            source: source_endpoint,
            destination: dest_endpoint,
            timestamp: Utc::now(),
            ..Default::default()
        };

        Ok(DryRunReport {
            run_id: uuid::Uuid::new_v4().to_string(),
            config_hash: params.config_hash.to_string(),
            engine_version: env!("CARGO_PKG_VERSION").to_string(),
            summary,
            mapping: MappingReport::from_mapping(params.mapping, copy_columns),
            ..Default::default()
        })
    }

    fn source_endpoint(source: &Source) -> Result<EndpointType, ReportGenerationError> {
        match &source.primary {
            DataSource::Database(_) => Ok(EndpointType::Database {
                dialect: source.dialect().name(),
            }),
            DataSource::File(_) => {
                Ok(EndpointType::File {
                    format: "CSV".to_string(), // Currently CSV is the only supported file type
                })
            }
        }
    }

    fn dest_endpoint(destination: &Destination) -> Result<EndpointType, ReportGenerationError> {
        match &destination.data_dest {
            DataDestination::Database(_) => Ok(EndpointType::Database {
                dialect: destination.dialect().name(),
            }),
        }
    }
}
