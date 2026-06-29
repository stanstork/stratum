use crate::{
    drivers::postgres::{driver::PgDriver, params::PgParamStore, queries, row::PgRowDecoder},
    error::DriverError,
    sql::{filter::SqlFilter, query::generator::QueryGenerator, request::FetchRowsRequest},
    traits::{reader::DataReader, row_decoder::RowDecoder},
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

        Ok(rows
            .iter()
            .map(|row| PgRowDecoder(row).decode(&request.table))
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
        let row = client
            .query_one(queries::COUNT_ROWS_FAST, &[&table])
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
}
