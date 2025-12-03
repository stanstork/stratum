#[derive(Clone, Copy, Debug, Default)]
pub struct DbCapabilities {
    pub copy_streaming: bool, // COPY FROM STDIN / LOAD DATA INFILE
    pub upsert_native: bool,  // INSERT..ON CONFLICT / REPLACE
    pub transactions: bool,
    pub merge_statements: bool, // ANSI MERGE
    pub ddl_online: bool,       // create/alter without exclusive locks
    pub temp_tables: bool,
}
