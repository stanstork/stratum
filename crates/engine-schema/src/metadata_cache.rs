use crate::row_counter::RowCounter;
use crate::type_registry::Dialect;
use async_trait::async_trait;
use connectors::{
    error::DriverError,
    sql::{
        filter::SqlFilter,
        metadata::{
            capabilities::Capabilities,
            constraint::{CheckConstraintMetadata, UniqueConstraintMetadata},
            fk::ForeignKeyMetadata,
            index::IndexMetadata,
            table::TableMetadata,
        },
    },
    traits::{
        driver::{Driver, DriverInfo},
        introspector::SchemaIntrospector,
        reader::DataReader,
    },
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
    fk_metadata: RwLock<HashMap<String, Vec<ForeignKeyMetadata>>>,
    referencing_tables: RwLock<HashMap<String, Vec<String>>>,
    unique_constraints: RwLock<HashMap<String, Vec<UniqueConstraintMetadata>>>,
    check_constraints: RwLock<HashMap<String, Vec<CheckConstraintMetadata>>>,
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
            fk_metadata: RwLock::new(HashMap::new()),
            referencing_tables: RwLock::new(HashMap::new()),
            unique_constraints: RwLock::new(HashMap::new()),
            check_constraints: RwLock::new(HashMap::new()),
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

impl<D: SchemaIntrospector + DataReader + Send + Sync + 'static> Driver for MetadataCache<D> {
    fn info(&self) -> &DriverInfo {
        self.introspector.info()
    }

    fn version(&self) -> &str {
        self.introspector.version()
    }

    fn capabilities(&self) -> &Capabilities {
        self.introspector.capabilities()
    }
}

/// Read-through `SchemaIntrospector`: lets an `Arc<MetadataCache<D>>` stand in
/// for the raw driver anywhere an `Arc<dyn SchemaIntrospector>` is expected, so
/// the schema-planning / graph-expansion path introspects each source table
/// once per run instead of re-querying it for every plan step and lane.
#[async_trait]
impl<D: SchemaIntrospector + DataReader + Send + Sync + 'static> SchemaIntrospector
    for MetadataCache<D>
{
    async fn table_exists(&self, table: &str) -> Result<bool, DriverError> {
        MetadataCache::table_exists(self, table).await
    }

    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<String>, DriverError> {
        // Not cached: cheap, and the `schema` argument varies.
        self.driver().list_tables(schema).await
    }

    async fn table_metadata(&self, table: &str) -> Result<TableMetadata, DriverError> {
        MetadataCache::table_metadata(self, table).await
    }

    async fn index_metadata(&self, table: &str) -> Result<Vec<IndexMetadata>, DriverError> {
        MetadataCache::index_metadata(self, table).await
    }

    async fn fk_metadata(&self, table: &str) -> Result<Vec<ForeignKeyMetadata>, DriverError> {
        let key = table.to_string();
        let driver = self.driver();
        cached_try_get(&self.fk_metadata, &key, || async {
            driver.fk_metadata(table).await
        })
        .await
    }

    async fn referencing_tables(&self, table: &str) -> Result<Vec<String>, DriverError> {
        let key = table.to_string();
        let driver = self.driver();
        cached_try_get(&self.referencing_tables, &key, || async {
            driver.referencing_tables(table).await
        })
        .await
    }

    async fn table_size_bytes(&self, table: &str) -> Result<u64, DriverError> {
        // Inherent method deliberately does not cache (volatile).
        MetadataCache::table_size_bytes(self, table).await
    }

    async fn unique_constraint_metadata(
        &self,
        table: &str,
    ) -> Result<Vec<UniqueConstraintMetadata>, DriverError> {
        let key = table.to_string();
        let driver = self.driver();
        cached_try_get(&self.unique_constraints, &key, || async {
            driver.unique_constraint_metadata(table).await
        })
        .await
    }

    async fn check_constraint_metadata(
        &self,
        table: &str,
    ) -> Result<Vec<CheckConstraintMetadata>, DriverError> {
        let key = table.to_string();
        let driver = self.driver();
        cached_try_get(&self.check_constraints, &key, || async {
            driver.check_constraint_metadata(table).await
        })
        .await
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
