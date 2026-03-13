pub use engine_core::schema::metadata_cache::{CacheStats, MetadataCache};

use connectors::drivers::{mysql::driver::MySqlDriver, postgres::driver::PgDriver};
use engine_core::{drivers::DriverRef, schema::type_registry::Dialect};
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
}
