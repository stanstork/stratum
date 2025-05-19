use crate::adapter::CsvAdapter;
use common::types::DataType;
use std::sync::Arc;

pub trait MetadataHelper {
    fn adapter(&self) -> Arc<CsvAdapter>;
}

#[derive(Debug, Clone)]
pub struct CsvColumnMetadata {
    pub name: String,
    pub data_type: DataType,
    pub is_nullable: bool,
}

#[derive(Debug, Clone)]
pub struct CsvMetadata {
    pub name: String,
    pub columns: Vec<CsvColumnMetadata>,
    pub delimiter: char,
    pub has_header: bool,
}
