#[derive(Debug, Clone)]
pub struct CsvColumnMetadata {
    pub name: String,
    pub data_type: String,
    pub is_nullable: bool,
}
