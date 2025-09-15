use crate::{
    error::MigrationError,
    report::dry_run::{DryRunParams, DryRunReport},
};
use smql::statements::setting::{CopyColumns, Settings};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

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

    /// Configures the migration for a dry run, generating a detailed report.
    pub fn mark_dry_run(&mut self, params: DryRunParams<'_>) -> Result<(), MigrationError> {
        let report = DryRunReport::new(params, &self.copy_columns)?;
        self.is_dry_run = true;
        self.dry_run_report = Arc::new(Mutex::new(report));
        info!("Migration marked as dry run.");
        Ok(())
    }

    pub fn dry_run_report(&self) -> Arc<Mutex<DryRunReport>> {
        self.dry_run_report.clone()
    }

    pub fn batch_size(&self) -> usize {
        self.batch_size
    }
}
