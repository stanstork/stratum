use model::core::types::Type;
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct CsvColumnMetadata {
    pub name: String,
    pub data_type: Type,
    pub is_nullable: bool,
    pub is_primary_key: bool,
    pub ordinal: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct CsvMetadata {
    pub name: String,
    pub columns: Vec<CsvColumnMetadata>,
    pub delimiter: char,
    pub has_header: bool,
}

pub fn normalize_col_name(name: &str) -> String {
    name.replace(" ", "_")
        .replace("-", "_")
        .replace(".", "_")
        .replace("(", "_")
        .replace(")", "_")
        .replace(",", "_")
        .to_lowercase()
}
