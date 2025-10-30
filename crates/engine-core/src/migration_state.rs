use smql_syntax::ast::setting::CopyColumns;

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
}

impl Default for MigrationState {
    fn default() -> Self {
        Self::new(false)
    }
}

impl MigrationState {
    pub fn new(dry_run: bool) -> Self {
        MigrationState {
            batch_size: 100,
            cascade_schema: false,
            copy_columns: CopyColumns::All,
            infer_schema: false,
            ignore_constraints: false,
            create_missing_columns: false,
            create_missing_tables: false,
            is_dry_run: dry_run,
        }
    }

    pub fn mark_dry_run(&mut self, dry_run: bool) {
        self.is_dry_run = dry_run;
    }

    pub fn is_dry_run(&self) -> bool {
        self.is_dry_run
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
