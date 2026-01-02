use super::column_parser::ColumnRef;
use crate::builder::infra::metadata_cache::MetadataCache;

/// Validates column references against schema metadata
pub struct ColumnValidator<'a> {
    source_cache: &'a MetadataCache,
}

impl<'a> ColumnValidator<'a> {
    pub fn new(source_cache: &'a MetadataCache) -> Self {
        Self { source_cache }
    }

    pub async fn exists(&self, col: &ColumnRef) -> bool {
        if let Ok(metadata) = self.source_cache.table_metadata(&col.table).await {
            // metadata.columns is a HashMap<String, ColumnMetadata>
            if metadata.columns.contains_key(&col.column) {
                return true;
            }
        }

        false
    }

    pub async fn all_exist(&self, cols: &[ColumnRef]) -> Result<(), Vec<String>> {
        let mut missing = Vec::new();

        for col in cols {
            if !self.exists(col).await {
                missing.push(format!("{}.{}", col.table, col.column));
            }
        }

        if missing.is_empty() {
            Ok(())
        } else {
            Err(missing)
        }
    }

    pub async fn any_exist(&self, cols: &[ColumnRef]) -> bool {
        for col in cols {
            if self.exists(col).await {
                return true;
            }
        }
        false
    }

    pub async fn partition_existing(&self, cols: &[ColumnRef]) -> (Vec<ColumnRef>, Vec<ColumnRef>) {
        let mut existing = Vec::new();
        let mut missing = Vec::new();

        for col in cols {
            if self.exists(col).await {
                existing.push(col.clone());
            } else {
                missing.push(col.clone());
            }
        }

        (existing, missing)
    }
}
