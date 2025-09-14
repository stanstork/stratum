use crate::{
    destination::{data::DataDestination, Destination},
    report::{
        dry_run::{DryRunReport, EndpointType},
        mapping::MappingReport,
    },
    source::{data::DataSource, Source},
};
use common::mapping::EntityMapping;
use smql::statements::setting::{CopyColumns, Settings};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
pub struct MigrationState {
    pub batch_size: usize,
    pub ignore_constraints: bool,
    pub infer_schema: bool,
    pub create_missing_columns: bool,
    pub create_missing_tables: bool,
    pub cascade_schema: bool,
    pub copy_columns: CopyColumns,
    pub is_dry_run: bool,
    pub dry_run_report: Arc<Mutex<DryRunReport>>,
}

impl MigrationState {
    pub fn from_settings(settings: &Settings) -> Self {
        MigrationState {
            batch_size: settings.batch_size,
            ignore_constraints: settings.ignore_constraints,
            infer_schema: settings.infer_schema,
            create_missing_columns: settings.create_missing_columns,
            create_missing_tables: settings.create_missing_tables,
            cascade_schema: settings.cascade_schema,
            copy_columns: settings.copy_columns.clone(),
            is_dry_run: false,
            dry_run_report: Arc::new(Mutex::new(DryRunReport::default())),
        }
    }

    pub async fn mark_dry_run(
        &mut self,
        source: &Source,
        destination: &Destination,
        mapping: &EntityMapping,
        config_hash: &str,
        dry_run: bool,
    ) {
        if !dry_run {
            return;
        }

        let report = Self::create_report(
            source,
            destination,
            mapping,
            config_hash,
            &self.copy_columns,
        )
        .await;
        self.is_dry_run = true;
        self.dry_run_report = Arc::new(Mutex::new(report));
    }

    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    pub async fn create_report(
        source: &Source,
        destination: &Destination,
        mapping: &EntityMapping,
        config_hash: &str,
        copy_columns: &CopyColumns,
    ) -> DryRunReport {
        let source_endpoint = match &source.primary {
            DataSource::Database(_) => EndpointType::Database {
                dialect: source.dialect().name(),
            },
            _ => panic!("Unsupported source type for dry run report"),
        };

        let dest_endpoint = match &destination.data_dest {
            DataDestination::Database(_) => EndpointType::Database {
                dialect: destination.dialect().name(),
            },
        };

        let mut report = DryRunReport::default();

        report.run_id = uuid::Uuid::new_v4().to_string();
        report.config_hash = config_hash.to_string();
        report.engine_version = env!("CARGO_PKG_VERSION").to_string();
        report.summary.source = source_endpoint;
        report.summary.destination = dest_endpoint;
        report.summary.timestamp = chrono::Utc::now();
        report.mapping = MappingReport::from_mapping(mapping, copy_columns);
        report
    }
}
