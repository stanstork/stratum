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
    pub settings: Settings,
    pub is_dry_run: bool,
    pub dry_run_report: Arc<Mutex<DryRunReport>>,
}

impl MigrationState {
    pub fn from_settings(settings: &Settings) -> Self {
        MigrationState {
            settings: settings.clone(),
            is_dry_run: false,
            dry_run_report: Arc::new(Mutex::new(DryRunReport::default())),
        }
    }

    /// Configures the migration for a dry run, generating a detailed report.
    pub fn mark_dry_run(&mut self, params: DryRunParams<'_>) -> Result<(), MigrationError> {
        let report = DryRunReport::new(params, &self.settings.copy_columns)?;
        self.is_dry_run = true;
        self.dry_run_report = Arc::new(Mutex::new(report));
        info!("Migration marked as dry run.");
        Ok(())
    }

    pub fn dry_run_report(&self) -> Arc<Mutex<DryRunReport>> {
        self.dry_run_report.clone()
    }

    pub fn set_batch_size(&mut self, size: usize) {
        self.settings.batch_size = size;
    }

    pub fn batch_size(&self) -> usize {
        self.settings.batch_size
    }

    pub fn set_cascade(&mut self, cascade: bool) {
        self.settings.cascade_schema = cascade;
    }

    pub fn cascade(&self) -> bool {
        self.settings.cascade_schema
    }

    pub fn set_copy_columns(&mut self, setting: CopyColumns) {
        self.settings.copy_columns = setting;
    }

    pub fn copy_columns(&self) -> CopyColumns {
        self.settings.copy_columns.clone()
    }

    pub fn set_infer_schema(&mut self, infer: bool) {
        self.settings.infer_schema = infer;
    }

    pub fn infer_schema(&self) -> bool {
        self.settings.infer_schema
    }

    pub fn set_ignore_constraints(&mut self, ignore: bool) {
        self.settings.ignore_constraints = ignore;
    }

    pub fn ignore_constraints(&self) -> bool {
        self.settings.ignore_constraints
    }

    pub fn set_create_missing_columns(&mut self, create: bool) {
        self.settings.create_missing_columns = create;
    }

    pub fn create_missing_columns(&self) -> bool {
        self.settings.create_missing_columns
    }

    pub fn set_create_missing_tables(&mut self, create: bool) {
        self.settings.create_missing_tables = create;
    }

    pub fn create_missing_tables(&self) -> bool {
        self.settings.create_missing_tables
    }
}
