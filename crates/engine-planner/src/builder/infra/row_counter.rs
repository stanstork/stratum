use crate::{builder::errors::RowCountError, plan::execution::types::RowCount};
use connectors::{
    adapter::Adapter,
    sql::base::{adapter::DatabaseKind, filter::SqlFilter},
};
use std::{sync::Arc, time::Duration};

pub struct RowCounter {
    adapter: Arc<Adapter>,
    _timeout: Duration, // TODO: pass timeout to adapter methods
}

impl RowCounter {
    pub fn new(adapter: Arc<Adapter>, timeout: Duration) -> Self {
        Self {
            adapter,
            _timeout: timeout,
        }
    }

    pub async fn count_rows(
        &self,
        table: &str,
        schema: Option<&str>,
        filter: Option<&SqlFilter>,
    ) -> Result<RowCount, RowCountError> {
        if filter.is_none() {
            // No filter: try fast methods first
            match self.count_fast(table, schema).await {
                Ok(count) => return Ok(count),
                Err(_) => {
                    tracing::debug!("Fast count failed for {}, trying approximate", table);
                }
            }

            match self.count_approximate(table, schema).await {
                Ok(count) => return Ok(count),
                Err(_) => {
                    tracing::debug!("Approximate count failed for {}, trying exact", table);
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
        schema: Option<&str>,
    ) -> Result<RowCount, RowCountError> {
        let sql_adapter = self.adapter.get_sql();
        let count = sql_adapter
            .count_rows_fast(table, schema)
            .await
            .map_err(|e| RowCountError::QueryFailed(e.to_string()))?;

        match sql_adapter.kind() {
            DatabaseKind::Postgres => {
                // PostgreSQL statistics are usually quite accurate
                Ok(RowCount::estimated(count, 0.9))
            }
            DatabaseKind::MySql => {
                // MySQL statistics can be very inaccurate depending on storage engine and settings
                Ok(RowCount::estimated(count, 0.8))
            }
            _ => Err(RowCountError::NotSupported),
        }
    }

    async fn count_exact(
        &self,
        table: &str,
        schema: Option<&str>,
        filter: Option<&SqlFilter>,
    ) -> Result<RowCount, RowCountError> {
        let count = self
            .adapter
            .get_sql()
            .count_rows(table, schema, filter)
            .await
            .map_err(|e| RowCountError::QueryFailed(e.to_string()))?;

        Ok(RowCount::exact(count))
    }

    async fn count_approximate(
        &self,
        table: &str,
        schema: Option<&str>,
    ) -> Result<RowCount, RowCountError> {
        let (sampled, _stats) = self
            .adapter
            .get_sql()
            .count_approximate(table, schema)
            .await
            .map_err(|e| RowCountError::QueryFailed(e.to_string()))?;

        // Use sampled estimate with 70% confidence
        Ok(RowCount::estimated(sampled, 0.7))
    }
}
