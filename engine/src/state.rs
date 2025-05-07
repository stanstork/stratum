use core::fmt;
use smql_v02::statements::setting::Settings;

pub struct MigrationState {
    pub batch_size: usize,
    pub ignore_constraints: bool,
    pub infer_schema: bool,
    pub create_missing_columns: bool,
    pub create_missing_tables: bool,
}

impl MigrationState {
    pub fn new() -> Self {
        MigrationState {
            batch_size: 100,
            ignore_constraints: false,
            infer_schema: false,
            create_missing_columns: false,
            create_missing_tables: false,
        }
    }

    pub fn from_settings(settings: &Settings) -> Self {
        MigrationState {
            batch_size: settings.batch_size,
            ignore_constraints: settings.ignore_constraints,
            infer_schema: settings.infer_schema,
            create_missing_columns: settings.create_missing_columns,
            create_missing_tables: settings.create_missing_tables,
        }
    }
}

impl fmt::Debug for MigrationState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "MigrationState {{ batch_size: {}, ignore_constraints: {}, 
            infer_schema: {}, create_missing_columns: {}, create_missing_tables: {} }}",
            self.batch_size,
            self.ignore_constraints,
            self.infer_schema,
            self.create_missing_columns,
            self.create_missing_tables
        )
    }
}
