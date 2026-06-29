use crate::row_counter::RowCounter;
use crate::type_registry::Dialect;
use connectors::{
    error::DriverError,
    sql::{
        filter::SqlFilter,
        metadata::{index::IndexMetadata, table::TableMetadata},
    },
    traits::{introspector::SchemaIntrospector, reader::DataReader},
};
use model::execution::row_count::RowCount;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::RwLock;
use tracing::{debug, error, warn};

/// A read-through cache: check the map, and on miss call `fetch` to populate.
async fn cached_get<K, V, F, Fut>(cache: &RwLock<HashMap<K, V>>, key: &K, fetch: F) -> V
where
    K: Clone + Eq + std::hash::Hash,
    V: Clone,
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = V>,
{
    // Fast path: read lock
    {
        let map = cache.read().await;
        if let Some(hit) = map.get(key) {
            return hit.clone();
        }
    }

    // Slow path: fetch + write lock
    let value = fetch().await;
    {
        let mut map = cache.write().await;
        map.insert(key.clone(), value.clone());
    }
    value
}

async fn cached_try_get<K, V, E, F, Fut>(
    cache: &RwLock<HashMap<K, V>>,
    key: &K,
    fetch: F,
) -> Result<V, E>
where
    K: Clone + Eq + std::hash::Hash,
    V: Clone,
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<V, E>>,
{
    // Fast path: read lock
    {
        let map = cache.read().await;
        if let Some(hit) = map.get(key) {
            return Ok(hit.clone());
        }
    }

    // Slow path: fetch + write lock (only on success)
    let value = fetch().await?;
    {
        let mut map = cache.write().await;
        map.insert(key.clone(), value.clone());
    }
    Ok(value)
}

/// Cache key for row counts (table + optional filter SQL)
#[derive(Clone, Hash, Eq, PartialEq)]
struct RowCountKey {
    table: String,
    filter_sql: String, // Use empty string for no filter
}

/// Cached metadata for database tables.
pub struct MetadataCache<D: SchemaIntrospector + DataReader + Send + Sync + 'static> {
    introspector: Arc<D>,
    row_counter: RowCounter<D>,

    table_metadata: RwLock<HashMap<String, TableMetadata>>,
    index_metadata: RwLock<HashMap<String, Vec<IndexMetadata>>>,
    table_exists: RwLock<HashMap<String, bool>>,
    row_counts: RwLock<HashMap<RowCountKey, RowCount>>,
}

impl<D: SchemaIntrospector + DataReader + Send + Sync + 'static> MetadataCache<D> {
    pub fn new(introspector: Arc<D>, dialect: Dialect, timeout: Duration) -> Self {
        let row_counter = RowCounter::new(Arc::clone(&introspector), dialect, timeout);
        Self {
            introspector,
            row_counter,
            table_metadata: RwLock::new(HashMap::new()),
            index_metadata: RwLock::new(HashMap::new()),
            table_exists: RwLock::new(HashMap::new()),
            row_counts: RwLock::new(HashMap::new()),
        }
    }

    pub fn driver(&self) -> Arc<D> {
        Arc::clone(&self.introspector)
    }

    pub async fn table_exists(&self, table: &str) -> Result<bool, DriverError> {
        let key = table.to_string();
        let driver = self.driver();

        cached_try_get(&self.table_exists, &key, || async {
            debug!(table = %table, "checking table existence");
            driver.table_exists(table).await.inspect_err(|e| {
                error!(table = %table, error = %e, "failed to check table existence");
            })
        })
        .await
    }

    pub async fn table_metadata(&self, table: &str) -> Result<TableMetadata, DriverError> {
        let key = table.to_string();
        let driver = self.driver();

        cached_try_get(&self.table_metadata, &key, || async {
            debug!(table = %table, "fetching table metadata");
            driver.table_metadata(table).await.inspect_err(|e| {
                error!(table = %table, error = %e, "failed to fetch table metadata");
            })
        })
        .await
    }

    pub async fn index_metadata(&self, table: &str) -> Result<Vec<IndexMetadata>, DriverError> {
        let key = table.to_string();
        let driver = self.driver();

        cached_try_get(&self.index_metadata, &key, || async {
            debug!(table = %table, "fetching index metadata");
            driver.index_metadata(table).await.inspect_err(|e| {
                error!(table = %table, error = %e, "failed to fetch index metadata");
            })
        })
        .await
    }

    pub async fn table_size_bytes(&self, table: &str) -> Result<u64, DriverError> {
        // Not cached — potentially volatile and cheap enough to re-fetch
        self.driver()
            .table_size_bytes(table)
            .await
            .inspect_err(|e| {
                error!(table = %table, error = %e, "failed to fetch table size");
            })
    }

    pub async fn count_rows(&self, table: &str, filter: Option<&SqlFilter>) -> RowCount {
        let key = RowCountKey {
            table: table.to_string(),
            filter_sql: filter.map(|f| f.to_sql()).unwrap_or_default(),
        };

        let row_counter = &self.row_counter;
        cached_get(&self.row_counts, &key, || async {
            row_counter
                .count_rows(table, None, filter)
                .await
                .unwrap_or_else(|e| {
                    warn!(table = %table, error = %e, "failed to get row count");
                    RowCount::unknown()
                })
        })
        .await
    }

    pub async fn is_column_indexed(&self, table: &str, column: &str) -> bool {
        let Ok(indexes) = self.index_metadata(table).await else {
            return false;
        };
        let col_lower = column.to_lowercase();
        indexes.iter().any(|idx| {
            idx.columns
                .iter()
                .any(|c| c.name.to_lowercase() == col_lower)
        })
    }

    pub async fn are_columns_indexed(&self, table: &str, columns: &[String]) -> bool {
        let Ok(indexes) = self.index_metadata(table).await else {
            return false;
        };
        let target: Vec<String> = columns.iter().map(|c| c.to_lowercase()).collect();
        indexes.iter().any(|idx| {
            idx.columns
                .iter()
                .any(|c| target.contains(&c.name.to_lowercase()))
        })
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
