use crate::type_registry::Dialect;
use connectors::{error::DriverError, sql::filter::SqlFilter, traits::reader::DataReader};
use model::execution::row_count::RowCount;
use std::{sync::Arc, time::Duration};

pub struct RowCounter<D: DataReader + Send + Sync + 'static> {
    introspector: Arc<D>,
    dialect: Dialect,
    _timeout: Duration, // TODO: pass timeout to adapter methods
}

impl<D: DataReader + Send + Sync + 'static> RowCounter<D> {
    pub fn new(introspector: Arc<D>, dialect: Dialect, timeout: Duration) -> Self {
        Self {
            introspector,
            dialect,
            _timeout: timeout,
        }
    }

    pub async fn count_rows(
        &self,
        table: &str,
        schema: Option<&str>,
        filter: Option<&SqlFilter>,
    ) -> Result<RowCount, DriverError> {
        if filter.is_none() {
            // No filter: try fast methods first
            match self.count_fast(table, schema).await {
                Ok(count) => return Ok(count),
                Err(_) => {
                    tracing::debug!("Fast count failed for {}, trying exact", table);
                }
            }
        }

        // With filter or as fallback: exact count
        self.count_exact(table, schema, filter).await
    }

    /// Fast count using table statistics (PostgreSQL pg_class, MySQL information_schema)
    async fn count_fast(
        &self,
        table: &str,
        _schema: Option<&str>,
    ) -> Result<RowCount, DriverError> {
        let count = self.introspector.count_fast(table).await?;

        let confidence = match self.dialect {
            Dialect::Postgres => 0.9, // PostgreSQL statistics are usually quite accurate
            Dialect::MySql => 0.8,    // MySQL statistics can be less accurate
        };
        Ok(RowCount::estimated(count, confidence))
    }

    async fn count_exact(
        &self,
        table: &str,
        schema: Option<&str>,
        filter: Option<&SqlFilter>,
    ) -> Result<RowCount, DriverError> {
        let count = self.introspector.count(table, schema, filter).await?;

        Ok(RowCount::exact(count))
    }
}
