use crate::{
    error::ReportGenerationError,
    report::dry_run::{DryRunParams, DryRunReport},
};
use smql::statements::setting::CopyColumns;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

#[derive(Debug, Clone)]
pub struct MigrationState {
    batch_size: usize,
    cascade_schema: bool,
    copy_columns: CopyColumns,
    infer_schema: bool,
    ignore_constraints: bool,
    create_missing_columns: bool,
    create_missing_tables: bool,
    is_dry_run: bool,
    dry_run_report: Arc<Mutex<DryRunReport>>,
}

impl Default for MigrationState {
    fn default() -> Self {
        Self::new()
    }
}

impl MigrationState {
    pub fn new() -> Self {
        MigrationState {
            batch_size: 100,
            cascade_schema: false,
            copy_columns: CopyColumns::All,
            infer_schema: false,
            ignore_constraints: false,
            create_missing_columns: false,
            create_missing_tables: false,
            is_dry_run: false,
            dry_run_report: Arc::new(Mutex::new(DryRunReport::default())),
        }
    }

    /// Configures the migration for a dry run, generating a detailed report.
    ///
    /// This method initializes a new `DryRunReport` and sets the `is_dry_run` flag.
    /// It returns an error if the report cannot be generated.
    pub fn mark_dry_run(&mut self, params: DryRunParams<'_>) -> Result<(), ReportGenerationError> {
        let report = DryRunReport::new(params, &self.copy_columns)?;
        self.is_dry_run = true;
        self.dry_run_report = Arc::new(Mutex::new(report));
        info!("Migration marked as dry run.");
        Ok(())
    }

    pub fn is_dry_run(&self) -> bool {
        self.is_dry_run
    }

    pub fn dry_run_report(&self) -> Arc<Mutex<DryRunReport>> {
        self.dry_run_report.clone()
    }

    pub fn set_batch_size(&mut self, size: usize) {
        self.batch_size = size;
    }

    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    pub fn set_cascade(&mut self, cascade: bool) {
        self.cascade_schema = cascade;
    }

    pub fn cascade(&self) -> bool {
        self.cascade_schema
    }

    pub fn set_copy_columns(&mut self, setting: CopyColumns) {
        self.copy_columns = setting;
    }

    pub fn copy_columns(&self) -> CopyColumns {
        self.copy_columns
    }

    pub fn set_infer_schema(&mut self, infer: bool) {
        self.infer_schema = infer;
    }

    pub fn infer_schema(&self) -> bool {
        self.infer_schema
    }

    pub fn set_ignore_constraints(&mut self, ignore: bool) {
        self.ignore_constraints = ignore;
    }

    pub fn ignore_constraints(&self) -> bool {
        self.ignore_constraints
    }

    pub fn set_create_missing_columns(&mut self, create: bool) {
        self.create_missing_columns = create;
    }

    pub fn create_missing_columns(&self) -> bool {
        self.create_missing_columns
    }

    pub fn set_create_missing_tables(&mut self, create: bool) {
        self.create_missing_tables = create;
    }

    pub fn create_missing_tables(&self) -> bool {
        self.create_missing_tables
    }
}
