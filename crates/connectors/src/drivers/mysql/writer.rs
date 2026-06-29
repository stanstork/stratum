use crate::{
    drivers::mysql::{driver::MySqlDriver, params::MySqlParamStore, types::MySqlTypeConverter},
    error::DriverError,
    sql::{metadata::table::TableMetadata, query::generator::QueryGenerator},
    traits::writer::DataWriter,
};
use async_trait::async_trait;
use model::records::Record;
use mysql_async::prelude::Queryable;
use query_builder::dialect;
use tracing::debug;

#[async_trait]
impl DataWriter for MySqlDriver {
    async fn write_batch(&self, meta: &TableMetadata, rows: &[Record]) -> Result<u64, DriverError> {
        if rows.is_empty() {
            return Ok(0);
        }

        let num_rows = rows.len();
        let generator = QueryGenerator::new(&dialect::MySql);
        let (sql, params) = generator.insert_batch(meta, rows, &MySqlTypeConverter);

        debug!(rows = num_rows, table = %meta.name, "inserting rows");

        let params = MySqlParamStore::from_values(&params).params();
        let mut conn = self.pool().get_conn().await?;
        let result = conn
            .exec_iter(&sql, params)
            .await
            .map_err(|e| DriverError::QueryError(format!("{:?}", e)))?;

        Ok(result.affected_rows())
    }
}
