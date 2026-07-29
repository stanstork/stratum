use crate::{
    drivers::postgres::{driver::PgDriver, params::PgParamStore, queries, row::PgRowDecoder},
    error::DriverError,
    sql::{filter::SqlFilter, query::generator::QueryGenerator, request::FetchRowsRequest},
    traits::{
        executor::QueryExecutor,
        introspector::SchemaIntrospector,
        reader::{DataReader, key_range_from_rows, single_int_pk},
        row_decoder::RowDecoder,
    },
};
use async_trait::async_trait;
use model::records::Record;
use query_builder::dialect;
use tracing::debug;

#[async_trait]
impl DataReader for PgDriver {
    async fn fetch(&self, request: FetchRowsRequest) -> Result<Vec<Record>, DriverError> {
        let generator = QueryGenerator::new(&dialect::Postgres);
        let (sql, params) = generator.select(&request);

        debug!(table = %request.table, filter = ?request.filter, "fetching rows");

        let client = self.client().read().await;
        let param_store = PgParamStore::from_values(&params);
        let rows = client
            .query(&sql, &param_store.as_refs()[..])
            .await
            .map_err(|e| DriverError::QueryError(e.to_string()))?;

        if rows.is_empty() {
            return Ok(Vec::new());
        }

        // Shared column schema built once; each row shares it.
        let schema = PgRowDecoder(&rows[0]).schema(&request.table);

        Ok(rows
            .iter()
            .map(|row| PgRowDecoder(row).decode_with_schema(&schema))
            .collect())
    }

    async fn count(
        &self,
        table: &str,
        schema: Option<&str>,
        filter: Option<&SqlFilter>,
    ) -> Result<u64, DriverError> {
        let fqn = queries::qualified_table_name(table, schema);

        let query = match filter {
            Some(f) => queries::COUNT
                .replace("{table}", &fqn)
                .replace("{filter}", &f.to_sql()),
            None => queries::COUNT_NO_FILTER.replace("{table}", &fqn),
        };

        let client = self.client().read().await;
        let row = client
            .query_one(&query, &[])
            .await
            .map_err(|e| DriverError::QueryError(e.to_string()))?;

        let count: i64 = row.get("count");
        Ok(count as u64)
    }

    async fn count_fast(&self, table: &str) -> Result<u64, DriverError> {
        let client = self.client().read().await;
        let schema = self.schema();
        let row = client
            .query_one(queries::COUNT_ROWS_FAST, &[&table, &schema])
            .await
            .map_err(|e| DriverError::QueryError(e.to_string()))?;

        let estimate: i64 = row.get("estimate");
        if estimate >= 0 {
            Ok(estimate as u64)
        } else {
            Err(DriverError::QueryError(
                "Negative row count estimate".to_string(),
            ))
        }
    }

    async fn int_key_range(&self, table: &str) -> Result<Option<(String, u64, u64)>, DriverError> {
        let meta = self.table_metadata(table).await?;
        let Some(pk) = single_int_pk(&meta, &dialect::Postgres) else {
            return Ok(None);
        };

        let (sql, _) = QueryGenerator::new(&dialect::Postgres).select_key_range(table, &pk);
        let rows = self.query(&sql).await?;

        Ok(key_range_from_rows(pk, &rows))
    }
}
