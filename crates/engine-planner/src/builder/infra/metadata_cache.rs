pub use engine_schema::metadata_cache::{CacheStats, MetadataCache};

use connectors::drivers::{mysql::driver::MySqlDriver, postgres::driver::PgDriver};
use engine_core::drivers::DriverRef;
use engine_schema::type_registry::Dialect;
use model::execution::row_count::RowCount;
use std::sync::Arc;

/// Unified handle for metadata cache (mirrors DriverRef variants).
#[derive(Clone)]
pub enum MetadataCacheRef {
    Postgres(Arc<MetadataCache<PgDriver>>),
    MySql(Arc<MetadataCache<MySqlDriver>>),
}

impl MetadataCacheRef {
    pub fn new(driver: &DriverRef, dialect: Dialect, timeout: std::time::Duration) -> Self {
        match driver {
            DriverRef::Postgres(d) => MetadataCacheRef::Postgres(Arc::new(MetadataCache::new(
                Arc::clone(d),
                dialect,
                timeout,
            ))),
            DriverRef::MySql(d) => MetadataCacheRef::MySql(Arc::new(MetadataCache::new(
                Arc::clone(d),
                dialect,
                timeout,
            ))),
        }
    }

    /// Total (unfiltered) row count for `table`, dispatched over the driver variant.
    pub async fn count_rows(&self, table: &str) -> RowCount {
        match self {
            MetadataCacheRef::Postgres(c) => c.count_rows(table, None).await,
            MetadataCacheRef::MySql(c) => c.count_rows(table, None).await,
        }
    }
}
