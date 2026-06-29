use crate::{
    drivers::mysql::{driver::MySqlDriver, params::MySqlParamStore, queries},
    error::DriverError,
    sql::{filter::SqlFilter, query::generator::QueryGenerator, request::FetchRowsRequest},
    traits::{reader::DataReader, row_decoder::RowDecoder},
};
use async_trait::async_trait;
use model::records::Record;
use mysql_async::{Row as MySqlRow, prelude::Queryable};
use query_builder::dialect;
use tracing::debug;

#[async_trait]
impl DataReader for MySqlDriver {
    async fn fetch(&self, request: FetchRowsRequest) -> Result<Vec<Record>, DriverError> {
        let generator = QueryGenerator::new(&dialect::MySql);
        let (sql, params) = generator.select(&request);

        debug!(sql = %sql, "generated SQL");

        let mut conn = self.pool().get_conn().await?;
        let params = MySqlParamStore::from_values(&params).params();
        let rows: Vec<MySqlRow> = conn.exec(sql, params).await?;
        Ok(rows.iter().map(|r| r.decode(&request.table)).collect())
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

        let mut conn = self.pool().get_conn().await?;
        let row: Option<MySqlRow> = conn.query_first(query).await?;
        let count: u64 = match row {
            Some(row) => row.get("count").unwrap_or(0),
            None => 0,
        };
        Ok(count)
    }

    async fn count_fast(&self, table: &str) -> Result<u64, DriverError> {
        let mut conn = self.pool().get_conn().await?;
        let row: Option<MySqlRow> = conn.exec_first(queries::COUNT_ROWS_FAST, (table,)).await?;
        let estimate: u64 = match row {
            Some(row) => row.get("estimate").unwrap_or(0),
            None => 0,
        };
        Ok(estimate)
    }
}
