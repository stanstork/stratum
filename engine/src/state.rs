pub struct MigrationState {
    pub batch_size: i64,
    pub infer_schema: bool,
}

impl MigrationState {
    pub fn new() -> Self {
        MigrationState {
            batch_size: 100,
            infer_schema: false,
        }
    }
}
