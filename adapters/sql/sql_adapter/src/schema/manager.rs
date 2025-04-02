use crate::metadata::table::TableMetadata;

pub trait SchemaManager: Send + Sync {
    fn get_metadata(&self, table: &str) -> &TableMetadata;
    fn set_metadata(&mut self, table: &str, metadata: TableMetadata);
}
