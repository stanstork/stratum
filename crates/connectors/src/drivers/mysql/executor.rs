use crate::{
    drivers::mysql::{driver::MySqlDriver, params::MySqlParamStore},
    error::DriverError,
    traits::{executor::QueryExecutor, row_decoder::RowDecoder},
};
use async_trait::async_trait;
use model::{core::value::Value, records::Record};
use mysql_async::{Row as MySqlRow, prelude::Queryable};

#[async_trait]
impl QueryExecutor for MySqlDriver {
    async fn execute(&self, sql: &str) -> Result<(), DriverError> {
        let mut conn = self.pool().get_conn().await?;
        conn.query_drop(sql).await?;
        Ok(())
    }

    async fn execute_params(&self, sql: &str, params: &[Value]) -> Result<(), DriverError> {
        let params = MySqlParamStore::from_values(params).params();
        let mut conn = self.pool().get_conn().await?;
        conn.exec_drop(sql, params).await?;
        Ok(())
    }

    async fn query(&self, sql: &str) -> Result<Vec<Record>, DriverError> {
        let mut conn = self.pool().get_conn().await?;
        let rows: Vec<MySqlRow> = conn.query(sql).await?;
        Ok(rows.iter().map(|r| r.decode("")).collect())
    }

    async fn query_params(&self, sql: &str, params: &[Value]) -> Result<Vec<Record>, DriverError> {
        let mut conn = self.pool().get_conn().await?;
        let params = MySqlParamStore::from_values(params).params();
        let rows: Vec<MySqlRow> = conn.exec(sql, params).await?;
        Ok(rows.iter().map(|r| r.decode("")).collect())
    }
}
