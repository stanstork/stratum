use smql::statements::setting::{CopyColumns, Settings};

#[derive(Debug)]
pub struct MigrationState {
    pub batch_size: usize,
    pub ignore_constraints: bool,
    pub infer_schema: bool,
    pub create_missing_columns: bool,
    pub create_missing_tables: bool,
    pub cascade_schema: bool,
    pub copy_columns: CopyColumns,
}

impl MigrationState {
    pub fn new() -> Self {
        MigrationState {
            batch_size: 100,
            ignore_constraints: false,
            infer_schema: false,
            create_missing_columns: false,
            create_missing_tables: false,
            cascade_schema: false,
            copy_columns: CopyColumns::All,
        }
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
        }
    }
}

impl Default for MigrationState {
    fn default() -> Self {
        MigrationState::new()
    }
}
