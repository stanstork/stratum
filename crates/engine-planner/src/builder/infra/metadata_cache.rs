use crate::{
    builder::{errors::SourceAnalyzerError, infra::row_counter::RowCounter},
    plan::execution::types::RowCount,
};
use connectors::{
    adapter::Adapter,
    sql::base::{
        filter::SqlFilter,
        metadata::{index::IndexMetadata, table::TableMetadata},
    },
};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::RwLock;
use tracing::{debug, error, warn};

/// Cache key for row counts (table + optional filter SQL)
#[derive(Clone, Hash, Eq, PartialEq)]
struct RowCountKey {
    table: String,
    filter_sql: String, // Use empty string for no filter
}

/// Cached metadata for database tables.
pub struct MetadataCache {
    adapter: Arc<Adapter>,
    row_counter: RowCounter,

    table_metadata: RwLock<HashMap<String, TableMetadata>>,
    index_metadata: RwLock<HashMap<String, Vec<IndexMetadata>>>,
    table_exists: RwLock<HashMap<String, bool>>,
    row_counts: RwLock<HashMap<RowCountKey, RowCount>>,
}

impl MetadataCache {
    pub fn new(adapter: Arc<Adapter>, timeout: Duration) -> Self {
        let row_counter = RowCounter::new(Arc::clone(&adapter), timeout);
        Self {
            adapter,
            row_counter,
            table_metadata: RwLock::new(HashMap::new()),
            index_metadata: RwLock::new(HashMap::new()),
            table_exists: RwLock::new(HashMap::new()),
            row_counts: RwLock::new(HashMap::new()),
        }
    }

    pub fn adapter(&self) -> &Arc<Adapter> {
        &self.adapter
    }

    pub async fn table_exists(&self, table: &str) -> Result<bool, SourceAnalyzerError> {
        // Check cache first
        {
            let cache = self.table_exists.read().await;
            if let Some(&exists) = cache.get(table) {
                debug!(table = %table, cached = true, "Table exists check (cached)");
                return Ok(exists);
            }
        }

        // Fetch from database
        let exists = self
            .adapter
            .get_sql()
            .table_exists(table)
            .await
            .map_err(|e| {
                error!(table = %table, error = %e, "Failed to check table existence");
                SourceAnalyzerError::IntrospectionFailed {
                    table: table.to_string(),
                    reason: format!("Table existence check failed: {}", e),
                }
            })?;

        // Cache the result
        {
            let mut cache = self.table_exists.write().await;
            cache.insert(table.to_string(), exists);
        }

        debug!(table = %table, exists = exists, "Table exists check");
        Ok(exists)
    }

    pub async fn table_metadata(&self, table: &str) -> Result<TableMetadata, SourceAnalyzerError> {
        // Check cache first
        {
            let cache = self.table_metadata.read().await;
            if let Some(metadata) = cache.get(table) {
                debug!(table = %table, cached = true, "Table metadata (cached)");
                return Ok(metadata.clone());
            }
        }

        // Fetch from database
        let metadata = self
            .adapter
            .get_sql()
            .table_metadata(table)
            .await
            .map_err(|e| {
                error!(table = %table, error = %e, "Failed to fetch table metadata");
                SourceAnalyzerError::IntrospectionFailed {
                    table: table.to_string(),
                    reason: format!("Metadata fetch failed: {}", e),
                }
            })?;

        // Cache the result
        {
            let mut cache = self.table_metadata.write().await;
            cache.insert(table.to_string(), metadata.clone());
        }

        debug!(table = %table, "Table metadata fetched");
        Ok(metadata)
    }

    pub async fn index_metadata(
        &self,
        table: &str,
    ) -> Result<Vec<IndexMetadata>, SourceAnalyzerError> {
        // Check cache first
        {
            let cache = self.index_metadata.read().await;
            if let Some(indexes) = cache.get(table) {
                debug!(table = %table, cached = true, count = indexes.len(), "Index metadata (cached)");
                return Ok(indexes.clone());
            }
        }

        // Fetch from database
        let indexes = self
            .adapter
            .get_sql()
            .index_metadata(table)
            .await
            .map_err(|e| {
                error!(table = %table, error = %e, "Failed to fetch table indexes");
                SourceAnalyzerError::IntrospectionFailed {
                    table: table.to_string(),
                    reason: format!("Index metadata fetch failed: {}", e),
                }
            })?;

        // Cache the result
        {
            let mut cache = self.index_metadata.write().await;
            cache.insert(table.to_string(), indexes.clone());
        }

        debug!(table = %table, count = indexes.len(), "Index metadata fetched");
        Ok(indexes)
    }

    pub async fn table_size_bytes(&self, table: &str) -> Result<u64, SourceAnalyzerError> {
        self.adapter
            .get_sql()
            .table_size_bytes(table)
            .await
            .map_err(|e| {
                error!(table = %table, error = %e, "Failed to fetch table size");
                SourceAnalyzerError::QueryFailed(format!("Table size fetch failed: {}", e))
            })
    }

    pub async fn count_rows(&self, table: &str, filter: Option<&SqlFilter>) -> RowCount {
        let filter_sql = filter.map(|f| f.to_sql()).unwrap_or_default();
        let key = RowCountKey {
            table: table.to_string(),
            filter_sql,
        };

        // Check cache first
        {
            let cache = self.row_counts.read().await;
            if let Some(count) = cache.get(&key) {
                debug!(table = %table, cached = true, "Row count (cached)");
                return count.clone();
            }
        }

        // Fetch from database
        let count = self
            .row_counter
            .count_rows(table, None, filter)
            .await
            .unwrap_or_else(|e| {
                warn!(table = %table, error = %e, "Failed to get row count");
                RowCount::unknown()
            });

        // Cache the result
        {
            let mut cache = self.row_counts.write().await;
            cache.insert(key, count.clone());
        }

        count
    }

    pub async fn is_column_indexed(&self, table: &str, column: &str) -> bool {
        match self.index_metadata(table).await {
            Ok(indexes) => {
                let col_lower = column.to_lowercase();
                indexes
                    .iter()
                    .any(|idx| idx.columns.iter().any(|c| c.to_lowercase() == col_lower))
            }
            Err(_) => false,
        }
    }

    pub async fn are_columns_indexed(&self, table: &str, columns: &[String]) -> bool {
        match self.index_metadata(table).await {
            Ok(indexes) => {
                let join_columns: Vec<String> = columns.iter().map(|c| c.to_lowercase()).collect();
                indexes.iter().any(|idx| {
                    idx.columns
                        .iter()
                        .any(|col| join_columns.contains(&col.to_lowercase()))
                })
            }
            Err(_) => false,
        }
    }

    /// Get cache statistics for debugging
    pub async fn stats(&self) -> CacheStats {
        CacheStats {
            table_metadata_entries: self.table_metadata.read().await.len(),
            index_metadata_entries: self.index_metadata.read().await.len(),
            table_exists_entries: self.table_exists.read().await.len(),
            row_count_entries: self.row_counts.read().await.len(),
        }
    }
}

/// Statistics about cache usage
#[derive(Debug)]
pub struct CacheStats {
    pub table_metadata_entries: usize,
    pub index_metadata_entries: usize,
    pub table_exists_entries: usize,
    pub row_count_entries: usize,
}
