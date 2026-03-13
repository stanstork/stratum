use crate::{
    drivers::postgres::{driver::PgDriver, params::PgParamStore, row::PgRowDecoder},
    error::DriverError,
    traits::{executor::QueryExecutor, row_decoder::RowDecoder},
};
use async_trait::async_trait;
use model::{core::value::Value, records::Record};

#[async_trait]
impl QueryExecutor for PgDriver {
    async fn execute(&self, sql: &str) -> Result<(), DriverError> {
        let client = self.client().read().await;
        client
            .batch_execute(sql)
            .await
            .map_err(|e| DriverError::QueryError(format!("{:?}", e)))?;
        Ok(())
    }

    async fn execute_params(&self, sql: &str, params: &[Value]) -> Result<(), DriverError> {
        let client = self.client().read().await;
        let param_store = PgParamStore::from_values(params);
        client
            .execute(sql, &param_store.as_refs()[..])
            .await
            .map_err(|e| DriverError::QueryError(format!("{:?}", e)))?;
        Ok(())
    }

    async fn query(&self, sql: &str) -> Result<Vec<Record>, DriverError> {
        let client = self.client().read().await;
        let rows = client
            .query(sql, &[])
            .await
            .map_err(|e| DriverError::QueryError(format!("{:?}", e)))?;

        Ok(rows
            .iter()
            .map(|row| PgRowDecoder(row).decode(""))
            .collect())
    }

    async fn query_params(&self, sql: &str, params: &[Value]) -> Result<Vec<Record>, DriverError> {
        let client = self.client().read().await;
        let param_store = PgParamStore::from_values(params);
        let rows = client
            .query(sql, &param_store.as_refs()[..])
            .await
            .map_err(|e| DriverError::QueryError(format!("{:?}", e)))?;

        Ok(rows
            .iter()
            .map(|row| PgRowDecoder(row).decode(""))
            .collect())
    }
}
