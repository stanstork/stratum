use std::sync::Arc;

use crate::report::validation::ValidationReport;
use smql::statements::setting::{CopyColumns, Settings};
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
    pub is_validation_run: bool,
    pub validation_report: Arc<Mutex<ValidationReport>>,
}

impl MigrationState {
    pub fn new(settings: &Settings, is_validation_run: bool) -> Self {
        let mut state = Self::from_settings(settings);
        state.is_validation_run = is_validation_run;
        state.validation_report = Arc::new(Mutex::new(ValidationReport::default()));
        state
    }

    pub fn from_settings(settings: &Settings) -> Self {
        MigrationState {
            batch_size: settings.batch_size,
            ignore_constraints: settings.ignore_constraints,
            infer_schema: settings.infer_schema,
            create_missing_columns: settings.create_missing_columns,
            create_missing_tables: settings.create_missing_tables,
            cascade_schema: settings.cascade_schema,
            copy_columns: settings.copy_columns.clone(),
            is_validation_run: false,
            validation_report: Arc::new(Mutex::new(ValidationReport::default())),
        }
    }

    pub fn mark_validation_run(&mut self) {
        self.is_validation_run = true;
    }

    pub fn set_validation_report(&mut self, report: ValidationReport) {
        self.validation_report = Arc::new(Mutex::new(report));
    }

    pub fn get_validation_report(&mut self) -> Arc<Mutex<ValidationReport>> {
        Arc::clone(&self.validation_report)
    }
}
