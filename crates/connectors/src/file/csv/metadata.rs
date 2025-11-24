use crate::file::csv::adapter::CsvAdapter;
use model::core::data_type::DataType;
use serde::Serialize;
use std::sync::Arc;

pub trait MetadataHelper {
    fn adapter(&self) -> Arc<CsvAdapter>;
    fn set_metadata(&mut self, meta: CsvMetadata);
}

#[derive(Debug, Clone, Serialize)]
pub struct CsvColumnMetadata {
    pub name: String,
    pub data_type: DataType,
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
