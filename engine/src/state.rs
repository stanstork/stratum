use core::fmt;

pub struct MigrationState {
    pub batch_size: usize,
    pub infer_schema: bool,
    pub create_missing_columns: bool,
    pub create_missing_tables: bool,
}

impl MigrationState {
    pub fn new() -> Self {
        MigrationState {
            batch_size: 100,
            infer_schema: false,
            create_missing_columns: false,
            create_missing_tables: false,
        }
    }
}

impl fmt::Debug for MigrationState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "MigrationState {{ batch_size: {}, infer_schema: {}, create_missing_columns: {}, create_missing_tables: {} }}",
            self.batch_size, self.infer_schema, self.create_missing_columns, self.create_missing_tables
        )
    }
}
