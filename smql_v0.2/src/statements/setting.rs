#[derive(Debug, Clone)]
pub struct Settings {
    pub infer_schema: bool,
    pub ignore_constraints: bool,
    pub create_missing_columns: bool,
    pub create_missing_tables: bool,
    pub copy_columns: CopyColumns,
    pub batch_size: usize,
}

#[derive(Debug, Clone)]
pub enum CopyColumns {
    All,
    MapOnly,
}
